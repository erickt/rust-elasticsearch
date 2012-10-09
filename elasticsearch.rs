export Transport, ZMQTransport, connect_with_zmq;
export Client;
export Consistency;
export Replication;
export OpType;
export VersionType;
export CreateIndexBuilder;
export DeleteIndexBuilder;
export IndexBuilder;
export SearchType;
export SearchBuilder;
export DeleteBuilder;
export DeleteByQueryBuilder;
export JsonObjectBuilder;
export JsonListBuilder;
export Response;

#[doc = "The low level interface to elasticsearch"]
trait Transport {
    fn head(path: &str) -> Response;
    fn get(path: &str) -> Response;
    fn put(path: &str, source: ~json::Object) -> Response;
    fn post(path: &str, source: ~json::Object) -> Response;
    fn delete(path: &str, source: Option<~json::Object>) -> Response;
    fn term();
}

#[doc = "The high level interface to elasticsearch"]
struct Client { transport: Transport }

#[doc = "Create an elasticsearch client"]
fn Client(transport: Transport) -> Client {
    Client { transport: transport }
}

impl Client {
    #[doc = "Create an index"]
    fn prepare_create_index(index: ~str) -> @CreateIndexBuilder {
        CreateIndexBuilder(self, index)
    }

    #[doc = "Delete indices"]
    fn prepare_delete_index() -> @DeleteIndexBuilder {
        DeleteIndexBuilder(self)
    }

    #[doc = "Get a specific document"]
    fn get(index: &str, typ: &str, id: &str) -> Response {
        let path = str::connect(~[
            net_url::encode_component(index),
            net_url::encode_component(typ),
            net_url::encode_component(id)
        ], "/");
        self.transport.get(path)
    }

    #[doc = "Create an index builder that will create documents"]
    fn prepare_index(+index: ~str, +typ: ~str) -> @IndexBuilder {
        IndexBuilder(self, index, typ)
    }

    #[doc = "Create a search builder that will query elasticsearch"]
    fn prepare_search() -> @SearchBuilder {
        SearchBuilder(self)
    }

    #[doc = "Delete a document"]
    fn delete(index: ~str, typ: ~str, id: ~str) -> Response {
        self.prepare_delete(index, typ, id).execute()
    }

    #[doc = "Delete a document"]
    fn prepare_delete(+index: ~str, +typ: ~str, +id: ~str) -> @DeleteBuilder {
        DeleteBuilder(self, index, typ, id)
    }

    #[doc = "Create a search builder that will query elasticsearch"]
    fn prepare_delete_by_query() -> @DeleteByQueryBuilder {
        DeleteByQueryBuilder(self)
    }

    #[doc = "Shut down the transport"]
    fn term() {
        self.transport.term();
    }
}

enum Consistency { One, Quorum, All }
enum Replication { Sync, Async }
enum OpType { CREATE, INDEX }
enum VersionType { INTERNAL, EXTERNAL }

struct CreateIndexBuilder {
    client: Client,
    index: ~str,

    mut timeout: Option<~str>,

    mut source: Option<@~json::Object>,
}

fn CreateIndexBuilder(client: Client, +index: ~str) -> @CreateIndexBuilder {
    @CreateIndexBuilder {
        client: client,
        index: index,

        mut timeout: None,

        mut source: None,
    }
}

impl CreateIndexBuilder {
    fn set_timeout(@self, +timeout: ~str) -> @CreateIndexBuilder {
        self.timeout = Some(timeout);
        self
    }
    fn set_source(@self, source: ~json::Object) -> @CreateIndexBuilder {
        self.source = Some(@source);
        self
    }
    fn execute() -> Response {
        let mut path = net_url::encode_component(self.index);

        let mut params = ~[];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { },
          Some(s) => params.push(~"timeout=" + s),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        match copy self.source {
          None => self.client.transport.put(path, ~LinearMap()),
          Some(source) => self.client.transport.put(path, *source),
        }
    }
}

struct DeleteIndexBuilder {
    client: Client,
    mut indices: ~[~str],
    mut timeout: Option<~str>,
}

fn DeleteIndexBuilder(client: Client) -> @DeleteIndexBuilder {
    @DeleteIndexBuilder {
        client: client,
        mut indices: ~[],
        mut timeout: None,
    }
}

impl DeleteIndexBuilder {
    fn set_indices(@self, +indices: ~[~str]) -> @DeleteIndexBuilder {
        self.indices = indices;
        self
    }
    fn set_timeout(@self, +timeout: ~str) -> @DeleteIndexBuilder {
        self.timeout = Some(timeout);
        self
    }
    fn execute() -> Response {
        let indices = do (copy self.indices).map |i| {
            net_url::encode_component(*i)
        };
        let mut path = str::connect(indices, ",");

        // Build the query parameters.
        let mut params = ~[];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { },
          Some(timeout) => params.push(~"timeout=" + timeout),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, None)
    }
}

struct IndexBuilder {
    client: Client,
    index: ~str,
    typ: ~str,
    mut id: Option<~str>,

    mut consistency: Option<Consistency>,
    mut op_type: OpType,
    mut parent: Option<~str>,
    mut percolate: Option<~str>,
    mut refresh: bool,
    mut replication: Option<Replication>,
    mut routing: Option<~str>,
    mut timeout: Option<~str>,
    mut timestamp: Option<~str>,
    mut ttl: Option<~str>,
    mut version: Option<uint>,
    mut version_type: VersionType,

    mut source: Option<~json::Object>,
}

fn IndexBuilder(client: Client, index: ~str, typ: ~str) -> @IndexBuilder {
    @IndexBuilder {
        client: client,
        index: index,
        typ: typ,
        mut id: None,

        mut consistency: None,
        mut op_type: INDEX,
        mut parent: None,
        mut percolate: None,
        mut refresh: false,
        mut replication: None,
        mut routing: None,
        mut timeout: None,
        mut timestamp: None,
        mut ttl: None,
        mut version: None,
        mut version_type: INTERNAL,

        mut source: None,
    }
}

impl IndexBuilder {
    fn set_id(@self, id: ~str) -> @IndexBuilder {
        self.id = Some(id);
        self
    }
    fn set_consistency(@self, consistency: Consistency) -> @IndexBuilder {
        self.consistency = Some(consistency);
        self
    }
    fn set_op_type(@self, op_type: OpType) -> @IndexBuilder {
        self.op_type = op_type;
        self
    }
    fn set_parent(@self, parent: ~str) -> @IndexBuilder {
        self.parent = Some(parent);
        self
    }
    fn set_percolate(@self, percolate: ~str) -> @IndexBuilder {
        self.percolate = Some(percolate);
        self
    }
    fn set_refresh(@self, refresh: bool) -> @IndexBuilder {
        self.refresh = refresh;
        self
    }
    fn set_replication(@self, replication: Replication) -> @IndexBuilder {
        self.replication = Some(replication);
        self
    }
    fn set_routing(@self, routing: ~str) -> @IndexBuilder {
        self.routing = Some(routing);
        self
    }
    fn set_timeout(@self, timeout: ~str) -> @IndexBuilder {
        self.timeout = Some(timeout);
        self
    }
    fn set_timestamp(@self, timestamp: ~str) -> @IndexBuilder {
        self.timestamp = Some(timestamp);
        self
    }
    fn set_ttl(@self, ttl: ~str) -> @IndexBuilder {
        self.ttl = Some(ttl);
        self
    }
    fn set_version(@self, version: uint) -> @IndexBuilder {
        self.version = Some(version);
        self
    }
    fn set_version_type(@self, version_type: VersionType) -> @IndexBuilder {
        self.version_type = version_type;
        self
    }
    fn set_source(@self, source: ~json::Object) -> @IndexBuilder {
        self.source = Some(source);
        self
    }
    fn execute() -> Response {
        let mut path = ~[
            net_url::encode_component(self.index),
            net_url::encode_component(self.typ)
        ];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.id {
          None => { },
          Some(id) => path.push(net_url::encode_component(id)),
        }

        let mut path = str::connect(path, "/");
        let mut params = ~[];

        match self.consistency {
            None => { },
            Some(One) => params.push(~"consistency=one"),
            Some(Quorum) => params.push(~"consistency=quorum"),
            Some(All) => params.push(~"consistency=all"),
        }

        match self.op_type {
          CREATE => params.push(~"op_type=create"),
          INDEX => { }
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.parent {
          None => { },
          Some(s) => params.push(~"parent=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.percolate {
          None => { }
          Some(s) =>  params.push(~"percolate=" + s),
        }

        if self.refresh { params.push(~"refresh=true"); }

        match self.replication {
          None => { },
          Some(Sync) => params.push(~"replication=sync"),
          Some(Async) => params.push(~"replication=async"),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.routing {
          None => { },
          Some(s) => params.push(~"routing=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { },
          Some(s) => params.push(~"timeout=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timestamp {
          None => { },
          Some(s) => params.push(~"timestamp=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.ttl {
          None => { },
          Some(s) => params.push(~"ttl=" + s),
        }

        (copy self.version).iter(|i| params.push(fmt!("version=%u", *i)));

        match self.version_type {
          INTERNAL => { },
          EXTERNAL => params.push(~"version_type=external"),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        let source = match self.source {
          None => ~LinearMap(),
          Some(source) => source,
        };

        match self.id {
          None => self.client.transport.post(path, source),
          Some(_) => self.client.transport.put(path, source),
        }
    }
}

enum SearchType {
    DfsQueryThenFetch,
    QueryThenFetch,
    DfsQueryAndFetch,
    QueryAndFetch,
    Scan,
    Count,
}

struct SearchBuilder {
    client: Client,
    mut indices: ~[~str],
    mut types: ~[~str],

    mut preference: Option<~str>,
    mut routing: Option<~str>,
    mut scroll: Option<~str>,
    mut search_type: Option<SearchType>,
    mut timeout: Option<~str>,

    mut source: Option<~json::Object>
}

fn SearchBuilder(client: Client) -> @SearchBuilder {
    @SearchBuilder {
        client: client,
        mut indices: ~[],
        mut types: ~[],

        mut preference: None,
        mut routing: None,
        mut scroll: None,
        mut search_type: None,
        mut timeout: None,

        mut source: None
    }
}

impl SearchBuilder {
    fn set_indices(@self, indices: ~[~str]) -> @SearchBuilder {
        self.indices = indices;
        self
    }
    fn set_types(@self, types: ~[~str]) -> @SearchBuilder {
        self.types = types;
        self
    }
    fn set_preference(@self, preference: ~str) -> @SearchBuilder {
        self.preference = Some(preference);
        self
    }
    fn set_routing(@self, routing: ~str) -> @SearchBuilder {
        self.routing = Some(routing);
        self
    }
    fn set_scroll(@self, scroll: ~str) -> @SearchBuilder {
        self.scroll = Some(scroll);
        self
    }
    fn set_search_type(@self, search_type: SearchType) -> @SearchBuilder {
        self.search_type = Some(search_type);
        self
    }
    fn set_timeout(@self, timeout: ~str) -> @SearchBuilder {
        self.timeout = Some(timeout);
        self
    }
    fn set_source(@self, source: ~json::Object) -> @SearchBuilder {
        self.source = Some(source);
        self
    }
    fn execute() -> Response {
        let indices = do (copy self.indices).map |i| {
            net_url::encode_component(*i)
        };

        let types = do (copy self.types).map |t| {
            net_url::encode_component(*t)
        };

        let mut path = ~[];

        path.push(str::connect(indices, ","));
        path.push(str::connect(types, ","));
        path.push(~"_search");

        let mut path = str::connect(path, "/");

        // Build the query parameters.
        let mut params = ~[];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.preference {
          None => { },
          Some(s) => params.push(~"preference=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.routing {
          None => { },
          Some(s) => params.push(~"routing=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.scroll {
          None => { },
          Some(s) => params.push(~"scroll=" + s),
        }

        match self.search_type {
            None => { },
            Some(DfsQueryThenFetch) =>
                params.push(~"search_type=dfs_query_then_fetch"),
            Some(QueryThenFetch) =>
                params.push(~"search_type=query_then_fetch"),
            Some(DfsQueryAndFetch) =>
                params.push(~"search_type=dfs_query_and_fetch"),
            Some(QueryAndFetch) =>
                params.push(~"search_type=query_and_fetch"),
            Some(Scan) => params.push(~"search_type=scan"),
            Some(Count) => params.push(~"search_type=count"),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { }
          Some(s) => params.push(~"timeout=" + s),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        let source = match self.source {
          None => ~LinearMap(),
          Some(source) => source,
        };

        self.client.transport.post(path, source)
    }
}

struct DeleteBuilder {
    client: Client,
    index: ~str,
    typ: ~str,
    id: ~str,

    mut consistency: Option<Consistency>,
    mut refresh: bool,
    mut replication: Option<Replication>,
    mut routing: Option<~str>,
    mut timeout: Option<~str>,
    mut version: Option<uint>,
    mut version_type: VersionType,
}

fn DeleteBuilder(
    client: Client,
    +index: ~str,
    +typ: ~str,
    +id: ~str
) -> @DeleteBuilder {
    @DeleteBuilder {
        client: client,
        index: index,
        typ: typ,
        id: id,
        mut consistency: None,
        mut refresh: false,
        mut replication: None,
        mut routing: None,
        mut timeout: None,
        mut version: None,
        mut version_type: INTERNAL
    }
}

impl DeleteBuilder {
    fn set_consistency(@self, consistency: Consistency) -> @DeleteBuilder {
        self.consistency = Some(consistency);
        self
    }
    fn set_parent(@self, parent: ~str) -> @DeleteBuilder {
        // We use the parent for routing.
        self.routing = Some(parent);
        self
    }
    fn set_refresh(@self, refresh: bool) -> @DeleteBuilder {
        self.refresh = refresh;
        self
    }
    fn set_replication(@self, replication: Replication) -> @DeleteBuilder {
        self.replication = Some(replication);
        self
    }
    fn set_routing(@self, routing: ~str) -> @DeleteBuilder {
        self.routing = Some(routing);
        self
    }
    fn set_timeout(@self, timeout: ~str) -> @DeleteBuilder {
        self.timeout = Some(timeout);
        self
    }
    fn set_version(@self, version: uint) -> @DeleteBuilder {
        self.version = Some(version);
        self
    }
    fn set_version_type(@self, version_type: VersionType) -> @DeleteBuilder {
        self.version_type = version_type;
        self
    }
    fn execute() -> Response {
        let mut path = str::connect(~[
            net_url::encode_component(self.index),
            net_url::encode_component(self.typ),
            net_url::encode_component(self.id)
        ], "/");

        // Build the query parameters.
        let mut params = ~[];

        match self.consistency {
            None => { },
            Some(One) => params.push(~"consistency=one"),
            Some(Quorum) => params.push(~"consistency=quorum"),
            Some(All) => params.push(~"consistency=all"),
        }

        if self.refresh { params.push(~"refresh=true"); }

        match self.replication {
          None => { }
          Some(Sync) => params.push(~"replication=sync"),
          Some(Async) => params.push(~"replication=async"),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.routing {
          None => { }
          Some(s) => params.push(~"routing=" + s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match copy self.timeout {
          None => { }
          Some(s) => params.push(~"timeout=" + s),
        }

        (copy self.version).iter(|i| params.push(fmt!("version=%u", *i)));

        match self.version_type {
          INTERNAL => { }
          EXTERNAL => params.push(~"version_type=external"),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, None)
    }
}

struct DeleteByQueryBuilder {
    client: Client,
    mut indices: ~[~str],
    mut types: ~[~str],

    mut consistency: Option<Consistency>,
    mut refresh: bool,
    mut replication: Option<Replication>,
    mut routing: Option<~str>,
    mut timeout: Option<~str>,

    mut source: Option<~json::Object>,
}

fn DeleteByQueryBuilder(client: Client) -> @DeleteByQueryBuilder {
    @DeleteByQueryBuilder {
        client: client,
        mut indices: ~[],
        mut types: ~[],

        mut consistency: None,
        mut refresh: false,
        mut replication: None,
        mut routing: None,
        mut timeout: None,

        mut source: None,
    }
}

impl DeleteByQueryBuilder {
    fn set_indices(@self, indices: ~[~str]) -> @DeleteByQueryBuilder {
        self.indices = indices;
        self
    }
    fn set_types(@self, types: ~[~str]) -> @DeleteByQueryBuilder {
        self.types = types;
        self
    }
    fn set_consistency(
        @self,
        consistency: Consistency
    ) -> @DeleteByQueryBuilder {
        self.consistency = Some(consistency);
        self
    }
    fn set_refresh(@self, refresh: bool) -> @DeleteByQueryBuilder {
        self.refresh = refresh;
        self
    }
    fn set_replication(
        @self,
        replication: Replication
    ) -> @DeleteByQueryBuilder {
        self.replication = Some(replication);
        self
    }
    fn set_routing(@self, routing: ~str) -> @DeleteByQueryBuilder {
        self.routing = Some(routing);
        self
    }
    fn set_timeout(@self, timeout: ~str) -> @DeleteByQueryBuilder {
        self.timeout = Some(timeout);
        self
    }
    fn set_source(
        @self,
        source: ~json::Object
    ) -> @DeleteByQueryBuilder {
        self.source = Some(source);
        self
    }

    fn execute() -> Response {
        let mut path = ~[];

        path.push(str::connect(self.indices, ","));
        path.push(str::connect(self.types, ","));
        path.push(~"_query");

        let mut path = str::connect(path, "/");

        // Build the query parameters.
        let mut params = ~[];

        match self.consistency {
            None => {}
            Some(One) => params.push(~"consistency=one"),
            Some(Quorum) => params.push(~"consistency=quorum"),
            Some(All) => params.push(~"consistency=all"),
        }

        if self.refresh { params.push(~"refresh=true"); }

        match self.replication {
          None => { }
          Some(Sync)  => params.push(~"replication=sync"),
          Some(Async) => params.push(~"replication=async"),
        }

        match copy self.routing {
          None => { }
          Some(routing) => params.push(~"routing=" + routing),
        }

        match copy self.timeout {
          None => { }
          Some(timeout) => params.push(~"timeout=" + timeout),
        }

        if vec::is_not_empty(params) {
            path += ~"?" + str::connect(params, "&");
        }

        self.client.transport.delete(path, copy self.source)
    }
}

pub struct JsonListBuilder {
    list: Cell<DVec<Json>>
}

pub fn JsonListBuilder() -> @JsonListBuilder {
    @JsonListBuilder { list: Cell(DVec()) }
}

priv impl JsonListBuilder {
    fn consume() -> ~[Json] {
        dvec::unwrap(self.list.take())
    }
}

pub impl JsonListBuilder {
    fn push<T: ToJson>(@self, value: T) -> @JsonListBuilder {
        self.list.with_ref(|list| list.push(value.to_json()));
        self
    }

    fn push_list(@self, f: fn(builder: @JsonListBuilder)) -> @JsonListBuilder {
        let builder = JsonListBuilder();
        f(builder);
        self.push(builder.consume())
    }

    fn push_object(
        @self,
        f: fn(builder: @JsonObjectBuilder)
    ) -> @JsonListBuilder {
        let builder = JsonObjectBuilder();
        f(builder);
        self.push(json::Object(builder.object.take()))
    }
}

pub struct JsonObjectBuilder { object: Cell<~json::Object> }

pub fn JsonObjectBuilder() -> @JsonObjectBuilder {
    @JsonObjectBuilder { object: Cell(~LinearMap()) }
}

pub impl JsonObjectBuilder {
    fn insert<T: ToJson>(@self, key: ~str, value: T) -> @JsonObjectBuilder {
        let mut object = self.object.take();
        object.insert(key, value.to_json());
        self.object.put_back(object);
        self
    }

    fn insert_list(
        @self,
        key: ~str,
        f: fn(builder: @JsonListBuilder)
    ) -> @JsonObjectBuilder {
        let builder = JsonListBuilder();
        f(builder);
        self.insert(key, builder.consume())
    }

    fn insert_object(
        @self,
        key: ~str,
        f: fn(builder: @JsonObjectBuilder)
    ) -> @JsonObjectBuilder {
        let builder = JsonObjectBuilder();
        f(builder);
        self.insert(key, json::Object(builder.object.take()))
    }
}

#[doc = "Transport to talk to Elasticsearch with zeromq"]
pub struct ZMQTransport { socket: zmq::Socket }

#[doc = "Zeromq transport implementation"]
pub impl ZMQTransport: Transport {
    fn head(path: &str) -> Response { self.send(fmt!("HEAD|%s", path)) }
    fn get(path: &str) -> Response { self.send(fmt!("GET|%s", path)) }
    fn put(path: &str, source: ~json::Object) -> Response {
        self.send(fmt!("PUT|%s|%s", path, json::Object(source).to_str()))
    }
    fn post(path: &str, source: ~json::Object) -> Response {
        self.send(fmt!("POST|%s|%s", path, json::Object(source).to_str()))
    }
    fn delete(path: &str, source: Option<~json::Object>) -> Response {
        match source {
          None => self.send(fmt!("DELETE|%s", path)),
          Some(source) =>
            self.send(fmt!("DELETE|%s|%s", path, json::Object(source).to_str())),
        }
    }

    fn send(request: &str) -> Response {
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
pub fn ZMQTransport(ctx: zmq::Context, addr: &str) -> Transport {
    let socket = ctx.socket(zmq::REQ);
    if socket.is_err() { fail socket.get_err().to_str() };
    let socket = result::unwrap(socket);

    match socket.connect(addr) {
      Ok(()) => { }
      Err(e) => fail e.to_str(),
    }

    ZMQTransport { socket: socket } as Transport
}

#[doc = "Helper function to creating a client with zeromq"]
pub fn connect_with_zmq(ctx: zmq::Context, addr: &str) -> Client {
    let transport = ZMQTransport(ctx, addr);
    Client(transport)
}

pub struct Response {
    code: uint,
    status: ~str,
    body: Json,
}

pub mod response {
    pub fn parse(msg: &[u8]) -> Response {
        let end = msg.len();

        let (start, code) = parse_code(msg, end);
        let (start, status) = parse_status(msg, start, end);
        let body = parse_body(msg, start, end);

        Response { code: code, status: status, body: body }
    }

    fn parse_code(msg: &[u8], end: uint) -> (uint, uint) {
        match vec::position_between(msg, 0u, end, |c| *c == '|' as u8) {
          None => fail ~"invalid response",
          Some(i) => {
            match uint::parse_bytes(vec::slice(msg, 0u, i), 10u) {
              Some(code) => (i + 1u, code),
              None => fail ~"invalid status code",
            }
          }
        }
    }

    fn parse_status(msg: &[u8], start: uint, end: uint) -> (uint, ~str) {
        match vec::position_between(msg, start, end, |c| *c == '|' as u8) {
          None => fail ~"invalid response",
          Some(i) => (i + 1u, str::from_bytes(vec::slice(msg, start, i))),
        }
    }

    fn parse_body(msg: &[u8], start: uint, end: uint) -> Json {
        if start == end { return json::Null; }

        do io::with_bytes_reader(vec::view(msg, start, end)) |rdr| {
            match json::from_reader(rdr) {
              Ok(json) => json,
              Err(e) => fail e.to_str(),
            }
        }
    }
}
