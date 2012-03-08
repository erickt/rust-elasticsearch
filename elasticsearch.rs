import result::{ok, err};
import std::io;
import std::json;
import zmq::{context, socket, error};

import json = std::json::json;

iface transport {
    fn head(path: str) -> response::t;
    fn get(path: str) -> response::t;
    fn put(path: str, doc: json) -> response::t;
    fn post(path: str, doc: json) -> response::t;
    fn delete(path: str) -> response::t;
}

type client = { transport: transport };

fn mk_client(transport: transport) -> client {
    { transport: transport }
}

#[doc = "Transport to talk to Elasticsearch with zeromq"]
type zmq_transport = { socket: zmq::socket };

#[doc = "Zeromq transport implementation"]
impl of transport for zmq_transport {
    fn head(path: str) -> response::t { self.send("HEAD|" + path) }
    fn get(path: str) -> response::t { self.send("GET|" + path) }
    fn put(path: str, doc: json) -> response::t {
        self.send("PUT|" + path + "|" + json::to_str(doc))
    }
    fn post(path: str, doc: json) -> response::t {
        self.send("POST|" + path + "|" + json::to_str(doc))
    }
    fn delete(path: str) -> response::t {
        self.send("DELETE|" + path)
    }

    fn send(request: str) -> response::t {
        str::as_bytes(request) { |bytes|
            alt self.socket.send_between(bytes, 0u, str::len(request), 0) {
              ok(()) {}
              err(e) { fail e.to_str(); }
            }
        }

        alt self.socket.recv(0) {
          ok(msg) { response::parse(msg) }
          err(e) { fail e.to_str(); }
        }
    }
}

#[doc = "Create a zeromq transport to Elasticsearch"]
fn mk_zmq_transport(ctx: zmq::context, addr: str) -> transport {
    let socket = alt ctx.socket(zmq::REQ) {
      ok(socket) { socket }
      err(e) { fail e.to_str() }
    };

    str::as_bytes(addr) { |bytes|
        alt socket.connect(bytes) {
          ok(()) {}
          err(e) { fail e.to_str(); }
        }
    }

    { socket: socket } as transport
}

#[doc = "Helper function to creating a client with zeromq"]
fn connect_with_zmq(ctx: zmq::context, addr: str) -> client {
    let transport = mk_zmq_transport(ctx, addr);
    mk_client(transport)
}

mod response {
    type t = {
        code: uint,
        status: str,
        doc: json,
    };

    fn parse(msg: [u8]) -> t {
        io::println(#fmt("parse: %?", str::from_bytes(msg)));

        let end = vec::len(msg);

        let (start, code) = parse_code(msg, end);
        let (start, status) = parse_status(msg, start, end);
        let doc = parse_doc(msg, start, end);

        { code: code, status: status, doc: doc }
    }

    fn parse_code(msg: [u8], end: uint) -> (uint, uint) {
        alt vec::position_from(msg, 0u, end) { |c| c == '|' as u8 } {
          none { fail "invalid response" }
          some(i) {
            alt uint::parse_buf(vec::slice(msg, 0u, i), 10u) {
              some(code) { (i + 1u, code) }
              none { fail "invalid status code" }
            }
          }
        }
    }

    fn parse_status(msg: [u8], start: uint, end: uint) -> (uint, str) {
        alt vec::position_from(msg, start, end) { |c| c == '|' as u8 } {
          none { fail "invalid response" }
          some(i) { (i + 1u, str::from_bytes(vec::slice(msg, start, i))) }
        }
    }

    fn parse_doc(msg: [u8], start: uint, end: uint) -> json {
        if start == end { ret json::null; }

        io::with_bytes_reader_between(msg, start, end) { |rdr|
            alt json::from_reader(rdr) {
              ok(json) { json }
              err({line, col, msg}) { fail #fmt("%u:%u: %s", line, col, msg); }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    import std::map;

    #[test]
    fn test() {
        let ctx = alt zmq::init(1) {
          ok(ctx) { ctx }
          err(e) { fail e.to_str(); }
        };

        let client = connect_with_zmq(ctx, "tcp://localhost:9700");
        io::println(#fmt("%?\n", client.transport.head("/")));
        io::println(#fmt("%?\n", client.transport.get("/")));

        let doc = map::new_str_hash();
        doc.insert("foo", json::string("bar"));

        io::println(#fmt("%?\n", client.transport.get("/test/test/1")));
        io::println(#fmt("%?\n", client.transport.put("/test/test/1", json::dict(doc))));
        io::println(#fmt("%?\n", client.transport.get("/test/test/1")));

        io::println(#fmt("%?\n", client.transport.delete("/test/test/1")));
    }
}
