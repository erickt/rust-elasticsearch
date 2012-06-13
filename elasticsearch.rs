export transport, zmq_transport, connect_with_zmq;
export client;
export consistency;
export replication;
export op_type;
export version_type;
export create_index_builder;
export delete_index_builder;
export index_builder;
export search_type;
export search_builder;
export delete_builder;
export delete_by_query_builder;
export json_dict_builder;
export json_list_builder;
export response;

#[doc = "The low level interface to elasticsearch"]
iface transport {
    fn head(path: str) -> response;
    fn get(path: str) -> response;
    fn put(path: str, source: hashmap<str, json>) -> response;
    fn post(path: str, source: hashmap<str, json>) -> response;
    fn delete(path: str, source: option<hashmap<str, json>>) -> response;
    fn term();
}

#[doc = "The high level interface to elasticsearch"]
type client = { transport: transport };

#[doc = "Create an elasticsearch client"]
fn client(transport: transport) -> client {
    { transport: transport }
}

impl client for client {
    #[doc = "Create an index"]
    fn prepare_create_index(+index: str) -> create_index_builder {
        create_index_builder(self, index)
    }

    #[doc = "Delete indices"]
    fn prepare_delete_index() -> delete_index_builder {
        delete_index_builder(self)
    }

    #[doc = "Get a specific document"]
    fn get(index: str, +typ: str, +id: str) -> response {
        let path = str::connect([
            encode_uri_component(index),
            encode_uri_component(typ),
            encode_uri_component(id)
        ], "/");
        self.transport.get(path)
    }

    #[doc = "Create an index builder that will create documents"]
    fn prepare_index(+index: str, +typ: str) -> index_builder {
        index_builder(self, index, typ)
    }

    #[doc = "Create a search builder that will query elasticsearch"]
    fn prepare_search() -> search_builder {
        search_builder(self)
    }

    #[doc = "Delete a document"]
    fn delete(+index: str, +typ: str, +id: str) -> response {
        self.prepare_delete(index, typ, id).execute()
    }

    #[doc = "Delete a document"]
    fn prepare_delete(+index: str, +typ: str, +id: str) -> delete_builder {
        delete_builder(self, index, typ, id)
    }

    #[doc = "Create a search builder that will query elasticsearch"]
    fn prepare_delete_by_query() -> delete_by_query_builder {
        delete_by_query_builder(self)
    }

    #[doc = "Shut down the transport"]
    fn term() {
        self.transport.term();
    }
}

enum consistency { CONSISTENCY_DEFAULT, ONE, QUORUM, ALL }
enum replication { REPLICATION_DEFAULT, SYNC, ASYNC }
enum op_type { CREATE, INDEX }
enum version_type { INTERNAL, EXTERNAL }

type create_index_builder = @{
    client: client,
    index: str,

    mut timeout: option<str>,

    mut source: option<@hashmap<str, json>>
};

fn create_index_builder(client: client, +index: str) -> create_index_builder {
    @{
        client: client,
        index: index,

        mut timeout: none,

        mut source: none,
    }
}

impl create_index_builder for create_index_builder {
    fn set_timeout(+timeout: str) -> create_index_builder {
        self.timeout = some(timeout);
        self
    }
    fn set_source(source: hashmap<str, json>) -> create_index_builder {
        self.source = some(@source);
        self
    }
    fn execute() -> response {
        let mut path = encode_uri_component(self.index);

        let mut params = [];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.timeout {
          none { }
          some(s) { vec::push(params, "timeout=" + s); }
        }

        if vec::is_not_empty(params) {
            path += "?" + str::connect(params, "&");
        }

        alt copy self.source {
          none { self.client.transport.put(path, map::str_hash()) }
          some(source) { self.client.transport.put(path, *source) }
        }
    }
}

type delete_index_builder = @{
    client: client,
    mut indices: [str],
    mut timeout: option<str>,
};

fn delete_index_builder(client: client) -> delete_index_builder {
    @{
        client: client,
        mut indices: [],
        mut timeout: none,
    }
}

impl delete_index_builder for delete_index_builder {
    fn set_indices(+indices: [str]) -> delete_index_builder {
        self.indices = indices;
        self
    }
    fn set_timeout(+timeout: str) -> delete_index_builder {
        self.timeout = some(timeout);
        self
    }
    fn execute() -> response {
        let indices = (copy self.indices).map { |i| encode_uri_component(i) };
        let mut path = str::connect(indices, ",");

        // Build the query parameters.
        let mut params = [];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.timeout {
          none { }
          some(timeout) { vec::push(params, "timeout=" + timeout); }
        }

        if vec::is_not_empty(params) {
            path += "?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, none)
    }
}

type index_builder = @{
    client: client,
    index: str,
    typ: str,
    mut id: option<str>,

    mut consistency: consistency,
    mut op_type: op_type,
    mut parent: option<str>,
    mut percolate: option<str>,
    mut refresh: bool,
    mut replication: replication,
    mut routing: option<str>,
    mut timeout: option<str>,
    mut timestamp: option<str>,
    mut ttl: option<str>,
    mut version: option<uint>,
    mut version_type: version_type,

    mut source: option<hashmap<str, json>>,
};

fn index_builder(client: client, +index: str, +typ: str) -> index_builder {
    @{
        client: client,
        index: index,
        typ: typ,
        mut id: none,

        mut consistency: CONSISTENCY_DEFAULT,
        mut op_type: INDEX,
        mut parent: none,
        mut percolate: none,
        mut refresh: false,
        mut replication: REPLICATION_DEFAULT,
        mut routing: none,
        mut timeout: none,
        mut timestamp: none,
        mut ttl: none,
        mut version: none,
        mut version_type: INTERNAL,

        mut source: none,
    }
}

impl index_builder for index_builder {
    fn set_id(+id: str) -> index_builder {
        self.id = some(id);
        self
    }
    fn set_consistency(consistency: consistency) -> index_builder {
        self.consistency = consistency;
        self
    }
    fn set_op_type(op_type: op_type) -> index_builder {
        self.op_type = op_type;
        self
    }
    fn set_parent(+parent: str) -> index_builder {
        self.parent = some(parent);
        self
    }
    fn set_percolate(+percolate: str) -> index_builder {
        self.percolate = some(percolate);
        self
    }
    fn set_refresh(refresh: bool) -> index_builder {
        self.refresh = refresh;
        self
    }
    fn set_replication(replication: replication) -> index_builder {
        self.replication = replication;
        self
    }
    fn set_routing(+routing: str) -> index_builder {
        self.routing = some(routing);
        self
    }
    fn set_timeout(+timeout: str) -> index_builder {
        self.timeout = some(timeout);
        self
    }
    fn set_timestamp(+timestamp: str) -> index_builder {
        self.timestamp = some(timestamp);
        self
    }
    fn set_ttl(+ttl: str) -> index_builder {
        self.ttl = some(ttl);
        self
    }
    fn set_version(version: uint) -> index_builder {
        self.version = some(version);
        self
    }
    fn set_version_type(version_type: version_type) -> index_builder {
        self.version_type = version_type;
        self
    }
    fn set_source(source: hashmap<str, json>) -> index_builder {
        self.source = some(source);
        self
    }
    fn execute() -> response {
        let mut path = [
            encode_uri_component(self.index),
            encode_uri_component(self.typ)
        ];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.id {
          none { }
          some(id) { vec::push(path, encode_uri_component(id)); }
        }

        let mut path = str::connect(path, "/");
        let mut params = [];

        alt self.consistency {
          CONSISTENCY_DEFAULT {}
          ONE { vec::push(params, "consistency=one"); }
          QUORUM { vec::push(params, "consistency=quorum"); }
          ALL { vec::push(params, "consistency=all"); }
        }

        alt self.op_type {
          CREATE { vec::push(params, "op_type=create"); }
          INDEX {}
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.parent {
          none { }
          some(s) { vec::push(params, "parent=" + s); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.percolate {
          none { }
          some(s) { vec::push(params, "percolate=" + s); }
        }

        if self.refresh { vec::push(params, "refresh=true"); }

        alt copy self.replication {
          REPLICATION_DEFAULT {}
          SYNC { vec::push(params, "replication=sync"); }
          ASYNC { vec::push(params, "replication=async"); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.routing {
          none { }
          some(s) { vec::push(params, "routing=" + s); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.timeout {
          none { }
          some(s) { vec::push(params, "timeout=" + s); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.timestamp {
          none { }
          some(s) { vec::push(params, "timestamp=" + s); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.ttl {
          none { }
          some(s) { vec::push(params, "ttl=" + s); }
        }

        (copy self.version).iter { |i|
            vec::push(params, #fmt("version=%u", i));
        }

        alt self.version_type {
          INTERNAL {}
          EXTERNAL { vec::push(params, "version_type=external"); }
        }

        if vec::is_not_empty(params) {
            path += "?" + str::connect(params, "&");
        }

        let source = alt self.source {
          none { map::str_hash() }
          some(source) { source }
        };

        alt self.id {
          none { self.client.transport.post(path, source) }
          some(_) { self.client.transport.put(path, source) }
        }
    }
}

enum search_type {
    SEARCH_DEFAULT,
    DFS_QUERY_THEN_FETCH,
    QUERY_THEN_FETCH,
    DFS_QUERY_AND_FETCH,
    QUERY_AND_FETCH,
    SCAN,
    COUNT,
}

type search_builder = @{
    client: client,
    mut indices: [str],
    mut types: [str],

    mut preference: option<str>,
    mut routing: option<str>,
    mut scroll: option<str>,
    mut search_type: search_type,
    mut timeout: option<str>,

    mut source: option<hashmap<str, json>>
};

fn search_builder(client: client) -> search_builder {
    @{
        client: client,
        mut indices: [],
        mut types: [],

        mut preference: none,
        mut routing: none,
        mut scroll: none,
        mut search_type: SEARCH_DEFAULT,
        mut timeout: none,

        mut source: none
    }
}

impl search_builder for search_builder {
    fn set_indices(+indices: [str]) -> search_builder {
        self.indices = indices;
        self
    }
    fn set_types(+types: [str]) -> search_builder {
        self.types = types;
        self
    }
    fn set_preference(+preference: str) -> search_builder {
        self.preference = some(preference);
        self
    }
    fn set_routing(+routing: str) -> search_builder {
        self.routing = some(routing);
        self
    }
    fn set_scroll(+scroll: str) -> search_builder {
        self.scroll = some(scroll);
        self
    }
    fn set_search_type(search_type: search_type) -> search_builder {
        self.search_type = search_type;
        self
    }
    fn set_timeout(+timeout: str) -> search_builder {
        self.timeout = some(timeout);
        self
    }
    fn set_source(source: hashmap<str, json>) -> search_builder {
        self.source = some(source);
        self
    }
    fn execute() -> response {
        let indices = (copy self.indices).map { |i| encode_uri_component(i) };
        let types   = (copy self.types).map   { |t| encode_uri_component(t) };

        let mut path = [];

        vec::push(path, str::connect(indices, ","));
        vec::push(path, str::connect(types, ","));
        vec::push(path, "_search");

        let mut path = str::connect(path, "/");

        // Build the query parameters.
        let mut params = [];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.preference {
          none { }
          some(s) { vec::push(params, "preference=" + s); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.routing {
          none { }
          some(s) { vec::push(params, "routing=" + s); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.scroll {
          none { }
          some(s) { vec::push(params, "scroll=" + s) }
        }

        alt self.search_type {
          SEARCH_DEFAULT {}
          DFS_QUERY_THEN_FETCH {
            vec::push(params, "search_type=dfs_query_then_fetch");
          }
          QUERY_THEN_FETCH {
            vec::push(params, "search_type=query_then_fetch");
          }
          DFS_QUERY_AND_FETCH {
            vec::push(params, "search_type=dfs_query_and_fetch");
          }
          QUERY_AND_FETCH { vec::push(params, "search_type=query_and_fetch"); }
          SCAN { vec::push(params, "search_type=scan"); }
          COUNT { vec::push(params, "search_type=count"); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.timeout {
          none { }
          some(s) { vec::push(params, "timeout=" + s); }
        }

        if vec::is_not_empty(params) {
            path += "?" + str::connect(params, "&");
        }

        let source : hashmap<str, json> = alt self.source {
          none { map::str_hash() }
          some(source) { source }
        };

        self.client.transport.post(path, source)
    }
}

type delete_builder = @{
    client: client,
    index: str,
    typ: str,
    id: str,

    mut consistency: consistency,
    mut refresh: bool,
    mut replication: replication,
    mut routing: option<str>,
    mut timeout: option<str>,
    mut version: option<uint>,
    mut version_type: version_type
};

fn delete_builder(
    client: client,
    +index: str,
    +typ: str,
    +id: str
) -> delete_builder {
    @{
        client: client,
        index: index,
        typ: typ,
        id: id,
        mut consistency: CONSISTENCY_DEFAULT,
        mut refresh: false,
        mut replication: REPLICATION_DEFAULT,
        mut routing: none,
        mut timeout: none,
        mut version: none,
        mut version_type: INTERNAL
    }
}

impl delete_builder for delete_builder {
    fn set_consistency(consistency: consistency) -> delete_builder {
        self.consistency = consistency;
        self
    }
    fn set_parent(+parent: str) -> delete_builder {
        // We use the parent for routing.
        self.routing = some(parent);
        self
    }
    fn set_refresh(refresh: bool) -> delete_builder {
        self.refresh = refresh;
        self
    }
    fn set_replication(replication: replication) -> delete_builder {
        self.replication = replication;
        self
    }
    fn set_routing(+routing: str) -> delete_builder {
        self.routing = some(routing);
        self
    }
    fn set_timeout(+timeout: str) -> delete_builder {
        self.timeout = some(timeout);
        self
    }
    fn set_version(version: uint) -> delete_builder {
        self.version = some(version);
        self
    }
    fn set_version_type(version_type: version_type) -> delete_builder {
        self.version_type = version_type;
        self
    }
    fn execute() -> response {
        let mut path = str::connect([
            encode_uri_component(self.index),
            encode_uri_component(self.typ),
            encode_uri_component(self.id)
        ], "/");

        // Build the query parameters.
        let mut params = [];

        alt self.consistency {
          CONSISTENCY_DEFAULT {}
          ONE { vec::push(params, "consistency=one"); }
          QUORUM { vec::push(params, "consistency=quorum"); }
          ALL { vec::push(params, "consistency=all"); }
        }

        if self.refresh { vec::push(params, "refresh=true"); }

        alt self.replication {
          REPLICATION_DEFAULT {}
          SYNC { vec::push(params, "replication=sync"); }
          ASYNC { vec::push(params, "replication=async"); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.routing {
          none { }
          some(s) { vec::push(params, "routing=" + s); }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        alt copy self.timeout {
          none { }
          some(s) { vec::push(params, "timeout=" + s); }
        }

        (copy self.version).iter { |i|
            vec::push(params, #fmt("version=%u", i));
        }

        alt self.version_type {
          INTERNAL {}
          EXTERNAL { vec::push(params, "version_type=external"); }
        }

        if vec::is_not_empty(params) {
            path += "?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, none)
    }
}

type delete_by_query_builder = @{
    client: client,
    mut indices: [str],
    mut types: [str],

    mut consistency: consistency,
    mut refresh: bool,
    mut replication: replication,
    mut routing: option<str>,
    mut timeout: option<str>,

    mut source: option<hashmap<str, json>>,
};

fn delete_by_query_builder(client: client) -> delete_by_query_builder {
    @{
        client: client,
        mut indices: [],
        mut types: [],

        mut consistency: CONSISTENCY_DEFAULT,
        mut refresh: false,
        mut replication: REPLICATION_DEFAULT,
        mut routing: none,
        mut timeout: none,

        mut source: none,
    }
}

impl delete_by_query_builder for delete_by_query_builder {
    fn set_indices(+indices: [str]) -> delete_by_query_builder {
        self.indices = indices;
        self
    }
    fn set_types(+types: [str]) -> delete_by_query_builder {
        self.types = types;
        self
    }
    fn set_consistency(consistency: consistency) -> delete_by_query_builder {
        self.consistency = consistency;
        self
    }
    fn set_refresh(refresh: bool) -> delete_by_query_builder {
        self.refresh = refresh;
        self
    }
    fn set_replication(replication: replication) -> delete_by_query_builder {
        self.replication = replication;
        self
    }
    fn set_routing(+routing: str) -> delete_by_query_builder {
        self.routing = some(routing);
        self
    }
    fn set_timeout(+timeout: str) -> delete_by_query_builder {
        self.timeout = some(timeout);
        self
    }
    fn set_source(source: hashmap<str, json>) -> delete_by_query_builder {
        self.source = some(source);
        self
    }
    fn execute() -> response {
        let mut path = [];

        vec::push(path, str::connect(self.indices, ","));
        vec::push(path, str::connect(self.types, ","));
        vec::push(path, "_query");

        let mut path = str::connect(path, "/");

        // Build the query parameters.
        let mut params = [];

        alt self.consistency {
          CONSISTENCY_DEFAULT {}
          ONE { vec::push(params, "consistency=one"); }
          QUORUM { vec::push(params, "consistency=quorum"); }
          ALL { vec::push(params, "consistency=all"); }
        }

        if self.refresh { vec::push(params, "refresh=true"); }

        alt self.replication {
          REPLICATION_DEFAULT {}
          SYNC { vec::push(params, "replication=sync"); }
          ASYNC { vec::push(params, "replication=async"); }
        }

        alt copy self.routing {
          none {}
          some(routing) { vec::push(params, "routing=" + routing); }
        }

        alt copy self.timeout {
          none {}
          some(timeout) { vec::push(params, "timeout=" + timeout); }
        }

        if vec::is_not_empty(params) {
            path += "?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, copy self.source)
    }
}

type json_list_builder = @dvec<json>;

fn json_list_builder() -> json_list_builder { 
    @dvec()
}

impl json_list_builder for json_list_builder {
    fn push<T: to_json>(value: T) -> json_list_builder {
        (*self).push(value.to_json());
        self
    }

    fn push_list(f: fn(json_list_builder)) -> json_list_builder {
        let builder = json_list_builder();
        f(builder);
        let builder <- *builder;
        self.push(vec::from_mut(dvec::unwrap(builder)))
    }

    fn push_dict(f: fn(json_dict_builder)) -> json_list_builder {
        let builder = json_dict_builder();
        f(builder);
        self.push(*builder)
    }
}


type json_dict_builder = @hashmap<str, json>;

fn json_dict_builder() -> json_dict_builder {
    @str_hash()
}

impl json_dict_builder for json_dict_builder {
    fn insert<T: to_json>(+key: str, value: T) -> json_dict_builder {
        (*self).insert(key, value.to_json());
        self
    }

    fn insert_list(+key: str, f: fn(json_list_builder)) -> json_dict_builder {
        let builder = json_list_builder();
        f(builder);
        let builder <- *builder;
        self.insert(key, vec::from_mut(dvec::unwrap(builder)))
    }

    fn insert_dict(+key: str, f: fn(json_dict_builder)) -> json_dict_builder {
        let builder = json_dict_builder();
        f(builder);
        self.insert(key, *builder)
    }
}

#[doc = "Transport to talk to Elasticsearch with zeromq"]
type zmq_transport = { socket: zmq::socket };

#[doc = "Zeromq transport implementation"]
impl of transport for zmq_transport {
    fn head(path: str) -> response { self.send("HEAD|" + path) }
    fn get(path: str) -> response { self.send("GET|" + path) }
    fn put(path: str, source: hashmap<str, json>) -> response {
        self.send("PUT|" + path + "|" + json::dict(source).to_str())
    }
    fn post(path: str, source: hashmap<str, json>) -> response {
        self.send("POST|" + path + "|" + json::dict(source).to_str())
    }
    fn delete(path: str, source: option<hashmap<str, json>>) -> response {
        alt source {
          none { self.send("DELETE|" + path) }
          some(source) {
            self.send("DELETE|" + path + "|" + json::dict(source).to_str())
          }
        }
    }

    fn send(request: str) -> response {
        #debug("request: %s", request);

        str::as_bytes(request) { |bytes|
            // The Elasticsearch ZMQ transpot expects there to be no trailing
            // \0.
            let bytes = vec::view(bytes, 0u, bytes.len() - 1u);

            alt self.socket.send(bytes, 0) {
              ok(()) {}
              err(e) { fail e.to_str(); }
            }
        }

        alt self.socket.recv(0) {
          ok(msg) {
            #debug("response: %s", str::from_bytes(msg));
            response::parse(msg)
          }
          err(e) { fail e.to_str(); }
        }
    }
    fn term() {
        self.socket.close();
    }
}

#[doc = "Create a zeromq transport to Elasticsearch"]
fn zmq_transport(ctx: zmq::context, addr: str) -> transport {
    let socket = alt ctx.socket(zmq::REQ) {
      ok(socket) { socket }
      err(e) { fail e.to_str() }
    };

    alt socket.connect(addr) {
      ok(()) {}
      err(e) { fail e.to_str(); }
    }

    { socket: socket } as transport
}

#[doc = "Helper function to creating a client with zeromq"]
fn connect_with_zmq(ctx: zmq::context, addr: str) -> client {
    let transport = zmq_transport(ctx, addr);
    client(transport)
}

type response = {
    code: uint,
    status: str,
    body: json,
};

mod response {
    fn parse(msg: [u8]) -> response {
        let end = vec::len(msg);

        let (start, code) = parse_code(msg, end);
        let (start, status) = parse_status(msg, start, end);
        let body = parse_body(msg, start, end);

        { code: code, status: status, body: body }
    }

    fn parse_code(msg: [u8], end: uint) -> (uint, uint) {
        alt vec::position_between(msg, 0u, end) { |c| c == '|' as u8 } {
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
        alt vec::position_between(msg, start, end) { |c| c == '|' as u8 } {
          none { fail "invalid response" }
          some(i) { (i + 1u, str::from_bytes(vec::slice(msg, start, i))) }
        }
    }

    fn parse_body(msg: [u8], start: uint, end: uint) -> json {
        if start == end { ret json::null; }

        io::with_bytes_reader_between(msg, start, end) { |rdr|
            alt json::from_reader(rdr) {
              ok(json) { json }
              err(e) { fail e.to_str(); }
            }
        }
    }
}
