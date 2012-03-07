import result::{ok, err};
import std::io;
import std::json;
import zmq::{context, socket, error};

type client = {
    socket: zmq::socket,
};

fn connect(ctx: zmq::context, addr: str) -> client {
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

    { socket: socket }
}

impl client for client {
    fn head(path: str) -> response::t { self.send("HEAD|" + path) }
    fn get(path: str) -> response::t { self.send("GET|" + path) }
    fn put(path: str, doc: json::json) -> response::t {
        self.send("PUT|" + path + "|" + json::to_str(doc))
    }
    fn post(path: str, doc: json::json) -> response::t {
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

mod response {
    type t = {
        code: uint,
        status: str,
        doc: json::json,
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

    fn parse_doc(msg: [u8], start: uint, end: uint)
      -> json::json {
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

        let client = connect(ctx, "tcp://localhost:9702");
        io::println(#fmt("%?\n", client.head("/")));
        io::println(#fmt("%?\n", client.get("/")));

        let doc = map::new_str_hash();
        doc.insert("foo", json::string("bar"));

        io::println(#fmt("%?\n", client.get("/test/test/1")));
        io::println(#fmt("%?\n", client.put("/test/test/1", json::dict(doc))));
        io::println(#fmt("%?\n", client.get("/test/test/1")));

        io::println(#fmt("%?\n", client.delete("/test/test/1")));
    }
}
