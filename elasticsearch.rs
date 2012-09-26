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
trait transport {
    fn head(path: &str) -> response;
    fn get(path: &str) -> response;
    fn put(path: &str, source: HashMap<~str, Json>) -> response;
    fn post(path: &str, source: HashMap<~str, Json>) -> response;
    fn delete(path: &str, source: Option<HashMap<~str, Json>>) -> response;
    fn term();
}

#[doc = "The high level interface to elasticsearch"]
struct client { transport: transport }

#[doc = "Create an elasticsearch client"]
fn client(transport: transport) -> client {
    client { transport: transport }
}

impl client {
    #[doc = "Create an index"]
    fn prepare_create_index(index: ~str) -> @create_index_builder {
        create_index_builder(self, index)
    }

    #[doc = "Delete indices"]
    fn prepare_delete_index() -> @delete_index_builder {
        delete_index_builder(self)
    }

    #[doc = "Get a specific document"]
    fn get(index: &str, typ: &str, id: &str) -> response {
        let path = str::connect(~[
            net_url::encode_component(index),
            net_url::encode_component(typ),
            net_url::encode_component(id)
        ], "/");
        self.transport.get(path)
    }

    #[doc = "Create an index builder that will create documents"]
    fn prepare_index(+index: ~str, +typ: ~str) -> @index_builder {
        index_builder(self, index, typ)
    }

    #[doc = "Create a search builder that will query elasticsearch"]
    fn prepare_search() -> @search_builder {
        search_builder(self)
    }

    #[doc = "Delete a document"]
    fn delete(index: ~str, typ: ~str, id: ~str) -> response {
        self.prepare_delete(index, typ, id).execute()
    }

    #[doc = "Delete a document"]
    fn prepare_delete(+index: ~str, +typ: ~str, +id: ~str) -> @delete_builder {
        delete_builder(self, index, typ, id)
    }

    #[doc = "Create a search builder that will query elasticsearch"]
    fn prepare_delete_by_query() -> @delete_by_query_builder {
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

struct create_index_builder {
    client: client,
    index: ~str,

    mut timeout: Option<~str>,

    mut source: Option<@HashMap<~str, Json>>,
}

fn create_index_builder(client: client, +index: ~str) -> @create_index_builder {
    @create_index_builder {
        client: client,
        index: index,

        mut timeout: None,

        mut source: None,
    }
}

impl create_index_builder {
    fn set_timeout(+timeout: ~str) -> create_index_builder {
        self.timeout = Some(timeout);
        self
    }
    fn set_source(source: HashMap<~str, Json>) -> create_index_builder {
        self.source = Some(@source);
        self
    }
    fn execute() -> response {
        let mut path = net_url::encode_component(self.index);

        let mut params = ~[];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { },
          Some(s) => vec::push(params, ~"timeout=" + s),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        match copy self.source {
          None => self.client.transport.put(path, map::HashMap()),
          Some(source) => self.client.transport.put(path, *source),
        }
    }
}

struct delete_index_builder {
    client: client,
    mut indices: ~[~str],
    mut timeout: Option<~str>,
}

fn delete_index_builder(client: client) -> @delete_index_builder {
    @delete_index_builder {
        client: client,
        mut indices: ~[],
        mut timeout: None,
    }
}

impl delete_index_builder {
    fn set_indices(+indices: ~[~str]) -> delete_index_builder {
        self.indices = indices;
        self
    }
    fn set_timeout(+timeout: ~str) -> delete_index_builder {
        self.timeout = Some(timeout);
        self
    }
    fn execute() -> response {
        let indices = do (copy self.indices).map |i| {
            net_url::encode_component(*i)
        };
        let mut path = str::connect(indices, ",");

        // Build the query parameters.
        let mut params = ~[];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { },
          Some(timeout) => vec::push(params, ~"timeout=" + timeout),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, None)
    }
}

struct index_builder {
    client: client,
    index: ~str,
    typ: ~str,
    mut id: Option<~str>,

    mut consistency: consistency,
    mut op_type: op_type,
    mut parent: Option<~str>,
    mut percolate: Option<~str>,
    mut refresh: bool,
    mut replication: replication,
    mut routing: Option<~str>,
    mut timeout: Option<~str>,
    mut timestamp: Option<~str>,
    mut ttl: Option<~str>,
    mut version: Option<uint>,
    mut version_type: version_type,

    mut source: Option<HashMap<~str, Json>>,
}

fn index_builder(client: client, +index: ~str, +typ: ~str) -> @index_builder {
    @index_builder {
        client: client,
        index: index,
        typ: typ,
        mut id: None,

        mut consistency: CONSISTENCY_DEFAULT,
        mut op_type: INDEX,
        mut parent: None,
        mut percolate: None,
        mut refresh: false,
        mut replication: REPLICATION_DEFAULT,
        mut routing: None,
        mut timeout: None,
        mut timestamp: None,
        mut ttl: None,
        mut version: None,
        mut version_type: INTERNAL,

        mut source: None,
    }
}

impl index_builder {
    fn set_id(+id: ~str) -> index_builder {
        self.id = Some(id);
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
    fn set_parent(+parent: ~str) -> index_builder {
        self.parent = Some(parent);
        self
    }
    fn set_percolate(+percolate: ~str) -> index_builder {
        self.percolate = Some(percolate);
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
    fn set_routing(+routing: ~str) -> index_builder {
        self.routing = Some(routing);
        self
    }
    fn set_timeout(+timeout: ~str) -> index_builder {
        self.timeout = Some(timeout);
        self
    }
    fn set_timestamp(+timestamp: ~str) -> index_builder {
        self.timestamp = Some(timestamp);
        self
    }
    fn set_ttl(+ttl: ~str) -> index_builder {
        self.ttl = Some(ttl);
        self
    }
    fn set_version(version: uint) -> index_builder {
        self.version = Some(version);
        self
    }
    fn set_version_type(version_type: version_type) -> index_builder {
        self.version_type = version_type;
        self
    }
    fn set_source(source: HashMap<~str, Json>) -> index_builder {
        self.source = Some(source);
        self
    }
    fn execute() -> response {
        let mut path = ~[
            net_url::encode_component(self.index),
            net_url::encode_component(self.typ)
        ];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.id {
          None => { },
          Some(id) => vec::push(path, net_url::encode_component(id)),
        }

        let mut path = str::connect(path, "/");
        let mut params = ~[];

        match self.consistency {
          CONSISTENCY_DEFAULT => { }
          ONE => vec::push(params, ~"consistency=one"),
          QUORUM => vec::push(params, ~"consistency=quorum"),
          ALL => vec::push(params, ~"consistency=all"),
        }

        match self.op_type {
          CREATE => vec::push(params, ~"op_type=create"),
          INDEX => { }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.parent {
          None => { },
          Some(s) => vec::push(params, ~"parent=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.percolate {
          None => { }
          Some(s) =>  vec::push(params, ~"percolate=" + s),
        }

        if self.refresh { vec::push(params, ~"refresh=true"); }

        match copy self.replication {
          REPLICATION_DEFAULT => { },
          SYNC => vec::push(params, ~"replication=sync"),
          ASYNC => vec::push(params, ~"replication=async"),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.routing {
          None => { },
          Some(s) => vec::push(params, ~"routing=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { },
          Some(s) => vec::push(params, ~"timeout=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timestamp {
          None => { },
          Some(s) => vec::push(params, ~"timestamp=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.ttl {
          None => { },
          Some(s) => vec::push(params, ~"ttl=" + s),
        }

        (copy self.version).iter(|i| vec::push(params, fmt!("version=%u", i)));

        match self.version_type {
          INTERNAL => { },
          EXTERNAL => vec::push(params, ~"version_type=external"),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        let source = match self.source {
          None => map::HashMap(),
          Some(source) => source,
        };

        match self.id {
          None => self.client.transport.post(path, source),
          Some(_) => self.client.transport.put(path, source),
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

struct search_builder {
    client: client,
    mut indices: ~[~str],
    mut types: ~[~str],

    mut preference: Option<~str>,
    mut routing: Option<~str>,
    mut scroll: Option<~str>,
    mut search_type: search_type,
    mut timeout: Option<~str>,

    mut source: Option<HashMap<~str, Json>>
}

fn search_builder(client: client) -> @search_builder {
    @search_builder {
        client: client,
        mut indices: ~[],
        mut types: ~[],

        mut preference: None,
        mut routing: None,
        mut scroll: None,
        mut search_type: SEARCH_DEFAULT,
        mut timeout: None,

        mut source: None
    }
}

impl search_builder {
    fn set_indices(+indices: ~[~str]) -> search_builder {
        self.indices = indices;
        self
    }
    fn set_types(+types: ~[~str]) -> search_builder {
        self.types = types;
        self
    }
    fn set_preference(+preference: ~str) -> search_builder {
        self.preference = Some(preference);
        self
    }
    fn set_routing(+routing: ~str) -> search_builder {
        self.routing = Some(routing);
        self
    }
    fn set_scroll(+scroll: ~str) -> search_builder {
        self.scroll = Some(scroll);
        self
    }
    fn set_search_type(search_type: search_type) -> search_builder {
        self.search_type = search_type;
        self
    }
    fn set_timeout(+timeout: ~str) -> search_builder {
        self.timeout = Some(timeout);
        self
    }
    fn set_source(source: HashMap<~str, Json>) -> search_builder {
        self.source = Some(source);
        self
    }
    fn execute() -> response {
        let indices = do (copy self.indices).map |i| {
            net_url::encode_component(*i)
        };

        let types = do (copy self.types).map |t| {
            net_url::encode_component(*t)
        };

        let mut path = ~[];

        vec::push(path, str::connect(indices, ","));
        vec::push(path, str::connect(types, ","));
        vec::push(path, ~"_search");

        let mut path = str::connect(path, "/");

        // Build the query parameters.
        let mut params = ~[];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.preference {
          None => { },
          Some(s) => vec::push(params, ~"preference=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.routing {
          None => { },
          Some(s) => vec::push(params, ~"routing=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.scroll {
          None => { },
          Some(s) => vec::push(params, ~"scroll=" + s),
        }

        match self.search_type {
          SEARCH_DEFAULT => { }
          DFS_QUERY_THEN_FETCH =>
            vec::push(params, ~"search_type=dfs_query_then_fetch"),
          QUERY_THEN_FETCH =>
            vec::push(params, ~"search_type=query_then_fetch"),
          DFS_QUERY_AND_FETCH =>
            vec::push(params, ~"search_type=dfs_query_and_fetch"),
          QUERY_AND_FETCH => vec::push(params, ~"search_type=query_and_fetch"),
          SCAN => vec::push(params, ~"search_type=scan"),
          COUNT => vec::push(params, ~"search_type=count"),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { }
          Some(s) => vec::push(params, ~"timeout=" + s),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        let source : HashMap<~str, Json> = match self.source {
          None => map::HashMap(),
          Some(source) => source,
        };

        self.client.transport.post(path, source)
    }
}

struct delete_builder {
    client: client,
    index: ~str,
    typ: ~str,
    id: ~str,

    mut consistency: consistency,
    mut refresh: bool,
    mut replication: replication,
    mut routing: Option<~str>,
    mut timeout: Option<~str>,
    mut version: Option<uint>,
    mut version_type: version_type,
}

fn delete_builder(
    client: client,
    +index: ~str,
    +typ: ~str,
    +id: ~str
) -> @delete_builder {
    @delete_builder {
        client: client,
        index: index,
        typ: typ,
        id: id,
        mut consistency: CONSISTENCY_DEFAULT,
        mut refresh: false,
        mut replication: REPLICATION_DEFAULT,
        mut routing: None,
        mut timeout: None,
        mut version: None,
        mut version_type: INTERNAL
    }
}

impl delete_builder {
    fn set_consistency(consistency: consistency) -> delete_builder {
        self.consistency = consistency;
        self
    }
    fn set_parent(+parent: ~str) -> delete_builder {
        // We use the parent for routing.
        self.routing = Some(parent);
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
    fn set_routing(+routing: ~str) -> delete_builder {
        self.routing = Some(routing);
        self
    }
    fn set_timeout(+timeout: ~str) -> delete_builder {
        self.timeout = Some(timeout);
        self
    }
    fn set_version(version: uint) -> delete_builder {
        self.version = Some(version);
        self
    }
    fn set_version_type(version_type: version_type) -> delete_builder {
        self.version_type = version_type;
        self
    }
    fn execute() -> response {
        let mut path = str::connect(~[
            net_url::encode_component(self.index),
            net_url::encode_component(self.typ),
            net_url::encode_component(self.id)
        ], "/");

        // Build the query parameters.
        let mut params = ~[];

        match self.consistency {
          CONSISTENCY_DEFAULT => { }
          ONE => vec::push(params, ~"consistency=one"),
          QUORUM => vec::push(params, ~"consistency=quorum"),
          ALL => vec::push(params, ~"consistency=all"),
        }

        if self.refresh { vec::push(params, ~"refresh=true"); }

        match self.replication {
          REPLICATION_DEFAULT => { }
          SYNC => vec::push(params, ~"replication=sync"),
          ASYNC => vec::push(params, ~"replication=async"),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.routing {
          None => { }
          Some(s) => vec::push(params, ~"routing=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { }
          Some(s) => vec::push(params, ~"timeout=" + s),
        }

        (copy self.version).iter(|i| vec::push(params, fmt!("version=%u", i)));

        match self.version_type {
          INTERNAL => { }
          EXTERNAL => vec::push(params, ~"version_type=external"),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, None)
    }
}

struct delete_by_query_builder {
    client: client,
    mut indices: ~[~str],
    mut types: ~[~str],

    mut consistency: consistency,
    mut refresh: bool,
    mut replication: replication,
    mut routing: Option<~str>,
    mut timeout: Option<~str>,

    mut source: Option<HashMap<~str, Json>>,
}

fn delete_by_query_builder(client: client) -> @delete_by_query_builder {
    @delete_by_query_builder {
        client: client,
        mut indices: ~[],
        mut types: ~[],

        mut consistency: CONSISTENCY_DEFAULT,
        mut refresh: false,
        mut replication: REPLICATION_DEFAULT,
        mut routing: None,
        mut timeout: None,

        mut source: None,
    }
}

impl delete_by_query_builder {
    fn set_indices(+indices: ~[~str]) -> delete_by_query_builder {
        self.indices = indices;
        self
    }
    fn set_types(+types: ~[~str]) -> delete_by_query_builder {
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
    fn set_routing(+routing: ~str) -> delete_by_query_builder {
        self.routing = Some(routing);
        self
    }
    fn set_timeout(+timeout: ~str) -> delete_by_query_builder {
        self.timeout = Some(timeout);
        self
    }
    fn set_source(source: HashMap<~str, Json>) -> delete_by_query_builder {
        self.source = Some(source);
        self
    }

    fn execute() -> response {
        let mut path = ~[];

        vec::push(path, str::connect(self.indices, ","));
        vec::push(path, str::connect(self.types, ","));
        vec::push(path, ~"_query");

        let mut path = str::connect(path, "/");

        // Build the query parameters.
        let mut params = ~[];

        match self.consistency {
          CONSISTENCY_DEFAULT => { }
          ONE => vec::push(params, ~"consistency=one"),
          QUORUM => vec::push(params, ~"consistency=quorum"),
          ALL => vec::push(params, ~"consistency=all"),
        }

        if self.refresh { vec::push(params, ~"refresh=true"); }

        match self.replication {
          REPLICATION_DEFAULT => { }
          SYNC  => vec::push(params, ~"replication=sync"),
          ASYNC => vec::push(params, ~"replication=async"),
        }

        match copy self.routing {
          None => { }
          Some(routing) => vec::push(params, ~"routing=" + routing),
        }

        match copy self.timeout {
          None => { }
          Some(timeout) => vec::push(params, ~"timeout=" + timeout),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, copy self.source)
    }
}

pub struct json_list_builder {
    list: DVec<Json>
}

pub fn json_list_builder() -> @json_list_builder {
    @json_list_builder { list: dvec::DVec() }
}

priv impl json_list_builder {
    fn consume() -> ~[Json] {
        dvec::unwrap(self.list)
    }
}

pub impl json_list_builder {
    fn push<T: ToJson>(self, value: T) -> json_list_builder {
        self.list.push(value.to_json());
        self
    }

    fn push_list(self, f: fn(builder: &json_list_builder)) -> json_list_builder {
        let builder = json_list_builder();
        f(&*builder);
        self.push(builder.consume())
    }

    fn push_dict(self, f: fn(builder: &json_dict_builder)) -> json_list_builder {
        let builder = json_dict_builder();
        f(builder);
        self.push(builder.dict)
    }
}

pub struct json_dict_builder { dict: HashMap<~str, Json> }

pub fn json_dict_builder() -> @json_dict_builder {
    @json_dict_builder { dict: HashMap() }
}

pub impl json_dict_builder {
    fn insert<T: ToJson>(self, key: ~str, value: T) -> json_dict_builder {
        self.dict.insert(key, value.to_json());
        self
    }

    fn insert_list(self, key: ~str, f: fn(builder: &json_list_builder)) -> json_dict_builder {
        let builder = json_list_builder();
        f(&*builder);
        self.insert(key, builder.consume())
    }

    fn insert_dict(self, key: ~str, f: fn(builder: &json_dict_builder)) -> json_dict_builder {
        let builder = json_dict_builder();
        f(builder);
        self.insert(key, builder.dict)
    }
}

#[doc = "Transport to talk to Elasticsearch with zeromq"]
pub struct zmq_transport { socket: zmq::Socket }

#[doc = "Zeromq transport implementation"]
pub impl zmq_transport: transport {
    fn head(path: &str) -> response { self.send(fmt!("HEAD|%s", path)) }
    fn get(path: &str) -> response { self.send(fmt!("GET|%s", path)) }
    fn put(path: &str, source: HashMap<~str, Json>) -> response {
        self.send(fmt!("PUT|%s|%s", path, json::Dict(source).to_str()))
    }
    fn post(path: &str, source: HashMap<~str, Json>) -> response {
        self.send(fmt!("POST|%s|%s", path, json::Dict(source).to_str()))
    }
    fn delete(path: &str, source: Option<HashMap<~str, Json>>) -> response {
        match source {
          None => self.send(fmt!("DELETE|%s", path)),
          Some(source) =>
            self.send(fmt!("DELETE|%s|%s", path, json::Dict(source).to_str())),
        }
    }

    fn send(request: &str) -> response {
        #debug("request: %s", request);

        match self.socket.send_str(request, 0) {
          Ok(()) => { }
          Err(e) => fail e.to_str(),
        }

        match self.socket.recv(0) {
          Ok(msg) => {
            let bytes = msg.to_bytes();
            #debug("response: %s", str::from_bytes(bytes));
            response::parse(bytes)
          },
          Err(e) => fail e.to_str(),
        }
    }
    fn term() {
        self.socket.close();
    }
}

#[doc = "Create a zeromq transport to Elasticsearch"]
pub fn zmq_transport(ctx: zmq::Context, addr: &str) -> transport {
    let socket = ctx.socket(zmq::REQ);
    if socket.is_err() { fail socket.get_err().to_str() };
    let socket = result::unwrap(socket);

    match socket.connect(addr) {
      Ok(()) => { }
      Err(e) => fail e.to_str(),
    }

    zmq_transport { socket: socket } as transport
}

#[doc = "Helper function to creating a client with zeromq"]
pub fn connect_with_zmq(ctx: zmq::Context, addr: &str) -> client {
    let transport = zmq_transport(ctx, addr);
    client(transport)
}

pub type response = {
    code: uint,
    status: ~str,
    body: Json,
};

pub mod response {
    pub fn parse(msg: ~[u8]) -> response {
        let end = msg.len();

        let (start, code) = parse_code(msg, end);
        let (start, status) = parse_status(msg, start, end);
        let body = parse_body(msg, start, end);

        { code: code, status: status, body: body }
    }

    fn parse_code(msg: ~[u8], end: uint) -> (uint, uint) {
        match vec::position_between(msg, 0u, end, |c| c == '|' as u8) {
          None => fail ~"invalid response",
          Some(i) => {
            match uint::parse_bytes(vec::slice(msg, 0u, i), 10u) {
              Some(code) => (i + 1u, code),
              None => fail ~"invalid status code",
            }
          }
        }
    }

    fn parse_status(msg: ~[u8], start: uint, end: uint) -> (uint, ~str) {
        match vec::position_between(msg, start, end, |c| c == '|' as u8) {
          None => fail ~"invalid response",
          Some(i) => (i + 1u, str::from_bytes(vec::slice(msg, start, i))),
        }
    }

    fn parse_body(msg: ~[u8], start: uint, end: uint) -> Json {
        if start == end { return json::Null; }

        do io::with_bytes_reader(vec::view(msg, start, end)) |rdr| {
            match json::from_reader(rdr) {
              Ok(json) => json,
              Err(e) => fail e.to_str(),
            }
        }
    }
}
