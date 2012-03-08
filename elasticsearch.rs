import result::{ok, err};
import std::io;
import std::json;
import std::map;
import std::map::map;
import zmq::{context, socket, error};

import json = std::json::json;

iface transport {
    fn head(path: str) -> response::t;
    fn get(path: str) -> response::t;
    fn put(path: str, doc: map<str, json>) -> response::t;
    fn post(path: str, doc: map<str, json>) -> response::t;
    fn delete(path: str) -> response::t;
}

type client = { transport: transport };

fn mk_client(transport: transport) -> client {
    { transport: transport }
}

impl client for client {
    fn get(index: str, typ: str, id: str) -> response::t {
        let path = index + "/" + typ + "/" + id;
        self.transport.get(path)
    }
    fn prepare_index(index: str, typ: str) -> index_builder {
        mk_index_builder(self, index, typ)
    }
    fn delete(index: str, typ: str, id: str) -> response::t {
        let path = index + "/" + typ + "/" + id;
        self.transport.delete(path)
    }
}

enum consistency { CONSISTENCY_DEFAULT, ONE, QUORUM, ALL }
enum replication { REPLICATION_DEFAULT, SYNC, ASYNC }
enum op_type { CREATE, INDEX }
enum version_type { INTERNAL, EXTERNAL }

type index_builder = {
    client: client,
    index: str,
    typ: str,
    mut id: option<str>,
    mut routing: option<str>,
    mut parent: option<str>,
    mut timestamp: option<str>,
    mut ttl: option<str>,
    mut op_type: op_type,
    mut refresh: bool,
    mut version: uint,
    mut version_type: version_type,
    mut percolate: option<str>,
    mut consistency: consistency,
    mut replication: replication,
    mut source: option<map<str, json>>,
};

fn mk_index_builder(client: client, index: str, typ: str) -> index_builder {
    {
        client: client,
        index: index,
        typ: typ,
        mut id: none,
        mut routing: none,
        mut parent: none,
        mut timestamp: none,
        mut ttl: none,
        mut op_type: INDEX,
        mut refresh: false,
        mut version: 0u,
        mut version_type: INTERNAL,
        mut percolate: none,
        mut consistency: CONSISTENCY_DEFAULT,
        mut replication: REPLICATION_DEFAULT,
        mut source: none,
    }
}

impl index_builder for index_builder {
    fn set_id(id: str) -> index_builder { self.id = some(id); self }
    fn set_routing(routing: str) -> index_builder {
        self.routing = some(routing);
        self
    }
    fn set_parent(parent: str) -> index_builder {
        self.parent = some(parent);
        self
    }
    fn set_timestamp(timestamp: str) -> index_builder {
        self.timestamp = some(timestamp);
        self
    }
    fn set_ttl(ttl: str) -> index_builder {
        self.ttl = some(ttl);
        self
    }
    fn set_op_type(op_type: op_type) -> index_builder {
        self.op_type = op_type;
        self
    }
    fn set_refresh() -> index_builder { self.refresh = true; self }
    fn set_version(version: uint) -> index_builder {
        self.version = version;
        self
    }
    fn set_version_type(version_type: version_type) -> index_builder {
        self.version_type = version_type;
        self
    }
    fn set_percolate(percolate: str) -> index_builder {
        self.percolate = some(percolate);
        self
    }
    fn set_consistency(consistency: consistency) -> index_builder {
        self.consistency = consistency;
        self
    }
    fn set_replication(replication: replication) -> index_builder {
        self.replication = replication;
        self
    }
    fn set_source(source: map<str, json>) -> index_builder {
        self.source = some(source);
        self
    }
    fn execute() -> response::t {
        let path = [self.index, self.typ];
        alt self.id {
          none {}
          some(id) { vec::push(path, id); }
        }

        let path = str::connect(path, "/");
        let params = [];

        alt self.routing {
          none {}
          some(routing) { vec::push(params, "routing=" + routing); }
        }

        alt self.parent {
          none {}
          some(parent) { vec::push(params, "parent=" + parent); }
        }

        alt self.timestamp {
          none {}
          some(timestamp) { vec::push(params, "timestamp=" + timestamp); }
        }

        alt self.ttl {
          none {}
          some(ttl) { vec::push(params, "ttl=" + ttl); }
        }

        alt self.op_type {
          CREATE { vec::push(params, "op_type=create"); }
          INDEX { vec::push(params, "op_type=index"); }
        }

        if self.refresh {
            vec::push(params, "refresh=true");
        }

        alt self.percolate {
          none {}
          some(percolate) { vec::push(params, "percolate=" + percolate); }
        }

        alt self.consistency {
          CONSISTENCY_DEFAULT {}
          ONE { vec::push(params, "consistency=one"); }
          QUORUM { vec::push(params, "consistency=quorum"); }
          ALL { vec::push(params, "consistency=all"); }
        }

        alt self.replication {
          REPLICATION_DEFAULT {}
          SYNC { vec::push(params, "replication=sync"); }
          ASYNC { vec::push(params, "replication=async"); }
        }

        if vec::is_not_empty(params) {
            path += "?" + str::connect(params, "&");
        }

        let source = alt self.source {
          none { map::new_str_hash() }
          some(source) { source }
        };

        alt self.id {
          none { self.client.transport.put(path, source) }
          some(_) { self.client.transport.post(path, source) }
        }
    }
}

type json_dict_builder = { dict: map<str, json> };

fn mk_json_dict_builder() -> json_dict_builder {
    { dict: map::new_str_hash() }
}

impl json_dict_builder for json_dict_builder {
    fn field_float(key: str, value: float) -> json_dict_builder {
        self.dict.insert(key, json::num(value));
        self
    }
    fn field_str(key: str, value: str) -> json_dict_builder {
        self.dict.insert(key, json::string(value));
        self
    }
    fn field_bool(key: str, value: bool) -> json_dict_builder {
        self.dict.insert(key, json::boolean(value));
        self
    }
    fn field_null(key: str) -> json_dict_builder {
        self.dict.insert(key, json::null);
        self
    }
    fn field_dict(key: str, f: fn(json_dict_builder))
      -> json_dict_builder {
        let builder = mk_json_dict_builder();
        f(builder);
        self.dict.insert(key, json::dict(builder.dict));
        self
    }
    fn field_list(key: str, f: fn(json_list_builder)) -> json_dict_builder {
        let builder = mk_json_list_builder();
        f(builder);
        self.dict.insert(key, json::list(builder.list));
        self
    }
}

type json_list_builder = { mut list: [json] };

fn mk_json_list_builder() -> json_list_builder {
    { mut list: [] }
}

impl json_list_builder for json_list_builder {
    fn add_float(value: float) -> json_list_builder {
        vec::push(self.list, json::num(value));
        self
    }
    fn add_str(value: str) -> json_list_builder {
        vec::push(self.list, json::string(value));
        self
    }
    fn add_bool(value: bool) -> json_list_builder {
        vec::push(self.list, json::boolean(value));
        self
    }
    fn add_null() -> json_list_builder {
        vec::push(self.list, json::null);
        self
    }
    fn add_dict(f: fn(json_dict_builder)) -> json_list_builder {
        let builder = mk_json_dict_builder();
        f(builder);
        vec::push(self.list, json::dict(builder.dict));
        self
    }
    fn add_list(f: fn(json_list_builder)) -> json_list_builder {
        let builder = mk_json_list_builder();
        f(builder);
        vec::push(self.list, json::list(builder.list));
        self
    }
}

#[doc = "Transport to talk to Elasticsearch with zeromq"]
type zmq_transport = { socket: zmq::socket };

#[doc = "Zeromq transport implementation"]
impl of transport for zmq_transport {
    fn head(path: str) -> response::t { self.send("HEAD|" + path) }
    fn get(path: str) -> response::t { self.send("GET|" + path) }
    fn put(path: str, doc: map<str, json>) -> response::t {
        self.send("PUT|" + path + "|" + json::to_str(json::dict(doc)))
    }
    fn post(path: str, doc: map<str, json>) -> response::t {
        self.send("POST|" + path + "|" + json::to_str(json::dict(doc)))
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

        io::println(#fmt("%?\n", client.transport.get("/test/test/1")));

        io::println(#fmt("%?\n", client.prepare_index("test", "test")
          .set_id("1")
          .set_version(2u)
          .set_source(mk_json_dict_builder()
              .field_float("foo", 5.0)
              .field_str("bar", "wee")
              .field_dict("baz") { |bld|
                  bld.field_float("a", 2.0);
              }
              .field_list("boo") { |bld|
                  bld.add_float(1.0).add_str("zzz");
              }
              .dict
          )
          .execute()));

        io::println(#fmt("%?\n", client.transport.get("/test/test/1")));

        io::println(#fmt("%?\n", client.delete("test", "test", "1")));
    }
}
