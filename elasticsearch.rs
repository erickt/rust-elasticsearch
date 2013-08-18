#[link(name = "elasticsearch",
       vers = "0.1",
       uuid = "51a41dbe-d547-4804-8300-8a1b5d00d093")];
#[crate_type = "lib"];

extern mod extra;
extern mod zmq;

use std::io;
use std::str;
use std::uint;

use extra::json::{Json, ToJson};
use extra::json;
use extra::treemap::TreeMap;
use extra::url;

use zmq::{Context, Socket, ToStr};

/// The low level interface to elasticsearch
pub trait Transport {
    fn head(&self, path: &str) -> Response;
    fn get(&self, path: &str) -> Response;
    fn put(&self, path: &str, source: ~json::Object) -> Response;
    fn post(&self, path: &str, source: ~json::Object) -> Response;
    fn delete(&self, path: &str, source: Option<~json::Object>) -> Response;
}

/// The high level interface to elasticsearch
pub struct Client {
    transport: ~Transport
}

impl Client {
    /// Create an elasticsearch client
    pub fn new(transport: ~Transport) -> Client {
        Client { transport: transport }
    }

    /// Create an index
    pub fn prepare_create_index<'a>(&'a self, index: ~str) -> CreateIndexBuilder<'a> {
        CreateIndexBuilder::new(self, index)
    }

    /// Delete indices
    pub fn prepare_delete_index<'a>(&'a self) -> DeleteIndexBuilder<'a> {
        DeleteIndexBuilder::new(self)
    }

    /// Get a specific document
    pub fn get(&self, index: &str, typ: &str, id: &str) -> Response {
        let path = [
            url::encode_component(index),
            url::encode_component(typ),
            url::encode_component(id)
        ].connect("/");
        self.transport.get(path)
    }

    /// Create an index builder that will create documents
    pub fn prepare_index<'a>(&'a self, index: ~str, typ: ~str) -> IndexBuilder<'a> {
        IndexBuilder::new(self, index, typ)
    }

    /// Create a search builder that will query elasticsearch
    pub fn prepare_search<'a>(&'a self) -> SearchBuilder<'a> {
        SearchBuilder::new(self)
    }

    /// Delete a document
    pub fn delete(&self, index: ~str, typ: ~str, id: ~str) -> Response {
        self.prepare_delete(index, typ, id).execute()
    }

    /// Delete a document
    pub fn prepare_delete<'a>(&'a self, index: ~str, typ: ~str, id: ~str) -> DeleteBuilder<'a> {
        DeleteBuilder::new(self, index, typ, id)
    }

    /// Create a search builder that will query elasticsearch
    pub fn prepare_delete_by_query<'a>(&'a self) -> DeleteByQueryBuilder<'a> {
        DeleteByQueryBuilder::new(self)
    }
}

pub enum Consistency { One, Quorum, All }
pub enum Replication { Sync, Async }
pub enum OpType { CREATE, INDEX }
pub enum VersionType { INTERNAL, EXTERNAL }

pub struct CreateIndexBuilder<'self> {
    client: &'self Client,
    index: ~str,
    timeout: Option<~str>,
    source: Option<~json::Object>,
}

impl<'self> CreateIndexBuilder<'self> {
    pub fn new(client: &'self Client, index: ~str) -> CreateIndexBuilder<'self> {
        CreateIndexBuilder {
            client: client,
            index: index,
            timeout: None,
            source: None,
        }
    }

    pub fn set_timeout(self, timeout: ~str) -> CreateIndexBuilder<'self> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_source(self, source: ~json::Object) -> CreateIndexBuilder<'self> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }
    pub fn execute(&mut self) -> Response {
        let mut path = url::encode_component(self.index);

        let mut params = ~[];

        match self.timeout {
            None => { },
            Some(ref s) => {
                params.push(~"timeout=" + *s);
            }
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&"));
        }

        match self.source.take() {
            None => self.client.transport.put(path, ~TreeMap::new()),
            Some(source) => self.client.transport.put(path, source),
        }
    }
}

pub struct DeleteIndexBuilder<'self> {
    client: &'self Client,
    indices: ~[~str],
    timeout: Option<~str>,
}

impl<'self> DeleteIndexBuilder<'self> {
    pub fn new(client: &'self Client) -> DeleteIndexBuilder<'self> {
        DeleteIndexBuilder {
            client: client,
            indices: ~[],
            timeout: None,
        }
    }

    pub fn set_indices(self, indices: ~[~str]) -> DeleteIndexBuilder<'self> {
        let mut builder = self;
        builder.indices = indices;
        builder
    }
    pub fn set_timeout(self, timeout: ~str) -> DeleteIndexBuilder<'self> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn execute(&mut self) -> Response {
        let indices = do self.indices.map |i| {
            url::encode_component(*i)
        };
        let mut path = indices.connect(",");

        // Build the query parameters.
        let mut params = ~[];

        match self.timeout {
            None => { },
            Some(ref timeout) => params.push(~"timeout=" + *timeout),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&"));
        }

        self.client.transport.delete(path, None)
    }
}

pub struct IndexBuilder<'self> {
    client: &'self Client,
    index: ~str,
    typ: ~str,
    id: Option<~str>,

    consistency: Option<Consistency>,
    op_type: OpType,
    parent: Option<~str>,
    percolate: Option<~str>,
    refresh: bool,
    replication: Option<Replication>,
    routing: Option<~str>,
    timeout: Option<~str>,
    timestamp: Option<~str>,
    ttl: Option<~str>,
    version: Option<uint>,
    version_type: VersionType,

    source: Option<~json::Object>,
}

impl<'self> IndexBuilder<'self> {
    pub fn new(client: &'self Client, index: ~str, typ: ~str) -> IndexBuilder<'self> {
        IndexBuilder {
            client: client,
            index: index,
            typ: typ,
            id: None,

            consistency: None,
            op_type: INDEX,
            parent: None,
            percolate: None,
            refresh: false,
            replication: None,
            routing: None,
            timeout: None,
            timestamp: None,
            ttl: None,
            version: None,
            version_type: INTERNAL,

            source: None,
        }
    }

    pub fn set_id(self, id: ~str) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.id = Some(id);
        builder
    }
    pub fn set_consistency(self, consistency: Consistency) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.consistency = Some(consistency);
        builder
    }
    pub fn set_op_type(self, op_type: OpType) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.op_type = op_type;
        builder
    }
    pub fn set_parent(self, parent: ~str) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.parent = Some(parent);
        builder
    }
    pub fn set_percolate(self, percolate: ~str) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.percolate = Some(percolate);
        builder
    }
    pub fn set_refresh(self, refresh: bool) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.refresh = refresh;
        builder
    }
    pub fn set_replication(self, replication: Replication) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.replication = Some(replication);
        builder
    }
    pub fn set_routing(self, routing: ~str) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_timeout(self, timeout: ~str) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_timestamp(self, timestamp: ~str) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.timestamp = Some(timestamp);
        builder
    }
    pub fn set_ttl(self, ttl: ~str) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.ttl = Some(ttl);
        builder
    }
    pub fn set_version(self, version: uint) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.version = Some(version);
        builder
    }
    pub fn set_version_type(self, version_type: VersionType) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.version_type = version_type;
        builder
    }
    pub fn set_source(self, source: ~json::Object) -> IndexBuilder<'self> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }
    pub fn execute(&mut self) -> Response {
        let mut path = ~[
            url::encode_component(self.index),
            url::encode_component(self.typ)
        ];

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match self.id {
            None => { },
            Some(ref id) => path.push(url::encode_component(*id)),
        }

        let mut path = path.connect("/");
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

        match self.parent {
            None => { },
            Some(ref s) => params.push(~"parent=" + *s),
        }

        match self.percolate {
            None => { }
            Some(ref s) =>  params.push(~"percolate=" + *s),
        }

        if self.refresh { params.push(~"refresh=true"); }

        match self.replication {
            None => { },
            Some(Sync) => params.push(~"replication=sync"),
            Some(Async) => params.push(~"replication=async"),
        }

        match self.routing {
            None => { },
            Some(ref s) => params.push(~"routing=" + *s),
        }

        match self.timeout {
            None => { },
            Some(ref s) => params.push(~"timeout=" + *s),
        }

        match self.timestamp {
            None => { },
            Some(ref s) => params.push(~"timestamp=" + *s),
        }

        match self.ttl {
            None => { },
            Some(ref s) => params.push(~"ttl=" + *s),
        }

        match self.version {
            None => { },
            Some(ref i) => { params.push(fmt!("version=%u", *i)); }
        }

        match self.version_type {
            INTERNAL => { },
            EXTERNAL => params.push(~"version_type=external"),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&"));
        }

        let source = match self.source.take() {
            None => ~TreeMap::new(),
            Some(source) => source,
        };

        match self.id {
            None => self.client.transport.post(path, source),
            Some(_) => self.client.transport.put(path, source),
        }
    }
}

pub enum SearchType {
    DfsQueryThenFetch,
    QueryThenFetch,
    DfsQueryAndFetch,
    QueryAndFetch,
    Scan,
    Count,
}

pub struct SearchBuilder<'self> {
    client: &'self Client,
    indices: ~[~str],
    types: ~[~str],

    preference: Option<~str>,
    routing: Option<~str>,
    scroll: Option<~str>,
    search_type: Option<SearchType>,
    timeout: Option<~str>,

    source: Option<~json::Object>
}

impl<'self> SearchBuilder<'self> {
    pub fn new(client: &'self Client) -> SearchBuilder<'self> {
        SearchBuilder {
            client: client,
            indices: ~[],
            types: ~[],

            preference: None,
            routing: None,
            scroll: None,
            search_type: None,
            timeout: None,

            source: None
        }
    }

    pub fn set_indices(self, indices: ~[~str]) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.indices = indices;
        builder
    }
    pub fn set_types(self, types: ~[~str]) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.types = types;
        builder
    }
    pub fn set_preference(self, preference: ~str) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.preference = Some(preference);
        builder
    }
    pub fn set_routing(self, routing: ~str) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_scroll(self, scroll: ~str) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.scroll = Some(scroll);
        builder
    }
    pub fn set_search_type(self, search_type: SearchType) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.search_type = Some(search_type);
        builder
    }
    pub fn set_timeout(self, timeout: ~str) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_source(self, source: ~json::Object) -> SearchBuilder<'self> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }
    pub fn execute(&mut self) -> Response {
        let indices = do self.indices.map |i| {
            url::encode_component(*i)
        };

        let types = do self.types.map |t| {
            url::encode_component(*t)
        };

        let mut path = ~[];

        path.push(indices.connect(","));
        path.push(types.connect(","));
        path.push(~"_search");

        let mut path = path.connect("/");

        // Build the query parameters.
        let mut params = ~[];

        match self.preference {
            None => { },
            Some(ref s) => params.push(~"preference=" + *s),
        }

        match self.routing {
            None => { },
            Some(ref s) => params.push(~"routing=" + *s),
        }

        match self.scroll {
            None => { },
            Some(ref s) => params.push(~"scroll=" + *s),
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

        match self.timeout {
            None => { }
            Some(ref s) => params.push(~"timeout=" + *s),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&"));
        }

        let source = match self.source.take() {
            None => ~TreeMap::new(),
            Some(source) => source,
        };

        self.client.transport.post(path, source)
    }
}

pub struct DeleteBuilder<'self> {
    client: &'self Client,
    index: ~str,
    typ: ~str,
    id: ~str,

    consistency: Option<Consistency>,
    refresh: bool,
    replication: Option<Replication>,
    routing: Option<~str>,
    timeout: Option<~str>,
    version: Option<uint>,
    version_type: VersionType,
}

impl<'self> DeleteBuilder<'self> {
    pub fn new(client: &'self Client, index: ~str, typ: ~str, id: ~str) -> DeleteBuilder<'self> {
        DeleteBuilder {
            client: client,
            index: index,
            typ: typ,
            id: id,
            consistency: None,
            refresh: false,
            replication: None,
            routing: None,
            timeout: None,
            version: None,
            version_type: INTERNAL,
        }
    }

    pub fn set_consistency(self, consistency: Consistency) -> DeleteBuilder<'self> {
        let mut builder = self;
        builder.consistency = Some(consistency);
        builder
    }
    pub fn set_parent(self, parent: ~str) -> DeleteBuilder<'self> {
        // We use the parent for routing.
        let mut builder = self;
        builder.routing = Some(parent);
        builder
    }
    pub fn set_refresh(self, refresh: bool) -> DeleteBuilder<'self> {
        let mut builder = self;
        builder.refresh = refresh;
        builder
    }
    pub fn set_replication(self, replication: Replication) -> DeleteBuilder<'self> {
        let mut builder = self;
        builder.replication = Some(replication);
        builder
    }
    pub fn set_routing(self, routing: ~str) -> DeleteBuilder<'self> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_timeout(self, timeout: ~str) -> DeleteBuilder<'self> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_version(self, version: uint) -> DeleteBuilder<'self> {
        let mut builder = self;
        builder.version = Some(version);
        builder
    }
    pub fn set_version_type(self, version_type: VersionType) -> DeleteBuilder<'self> {
        let mut builder = self;
        builder.version_type = version_type;
        builder
    }
    pub fn execute(&mut self) -> Response {
        let mut path = [
            url::encode_component(self.index),
            url::encode_component(self.typ),
            url::encode_component(self.id)
        ].connect("/");

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
        match self.routing {
            None => { }
            Some(ref s) => params.push(~"routing=" + *s),
        }

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match self.timeout {
            None => { }
            Some(ref s) => params.push(~"timeout=" + *s),
        }

        match self.version {
            None => { }
            Some(ref s) => params.push(fmt!("version=%u", *s)),
        }

        match self.version_type {
            INTERNAL => { }
            EXTERNAL => params.push(~"version_type=external"),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&"));
        }

        self.client.transport.delete(path, None)
    }
}

pub struct DeleteByQueryBuilder<'self> {
    client: &'self Client,
    indices: ~[~str],
    types: ~[~str],

    consistency: Option<Consistency>,
    refresh: bool,
    replication: Option<Replication>,
    routing: Option<~str>,
    timeout: Option<~str>,

    source: Option<~json::Object>,
}

impl<'self> DeleteByQueryBuilder<'self> {
    pub fn new(client: &'self Client) -> DeleteByQueryBuilder<'self> {
        DeleteByQueryBuilder {
            client: client,
            indices: ~[],
            types: ~[],

            consistency: None,
            refresh: false,
            replication: None,
            routing: None,
            timeout: None,

            source: None,
        }
    }

    pub fn set_indices(self, indices: ~[~str]) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.indices = indices;
        builder
    }
    pub fn set_types(self, types: ~[~str]) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.types = types;
        builder
    }
    pub fn set_consistency(self, consistency: Consistency) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.consistency = Some(consistency);
        builder
    }
    pub fn set_refresh(self, refresh: bool) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.refresh = refresh;
        builder
    }
    pub fn set_replication(self, replication: Replication) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.replication = Some(replication);
        builder
    }
    pub fn set_routing(self, routing: ~str) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_timeout(self, timeout: ~str) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_source(self, source: ~json::Object) -> DeleteByQueryBuilder<'self> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }

    pub fn execute(&mut self) -> Response {
        let mut path = ~[];

        path.push(self.indices.connect(","));
        path.push(self.types.connect(","));
        path.push(~"_query");

        let mut path = path.connect("/");

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

        match self.routing {
            None => { }
            Some(ref routing) => params.push(~"routing=" + *routing),
        }

        match self.timeout {
            None => { }
            Some(ref timeout) => params.push(~"timeout=" + *timeout),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&"));
        }

        let source = self.source.take();

        self.client.transport.delete(path, source)
    }
}

pub struct JsonListBuilder {
    list: ~[Json]
}

impl JsonListBuilder {
    pub fn new() -> JsonListBuilder {
        JsonListBuilder { list: ~[] }
    }

    pub fn unwrap(self) -> ~[Json] {
        let JsonListBuilder { list } = self;
        list
    }

    pub fn push<T: ToJson>(self, value: T) -> JsonListBuilder {
        let mut builder = self;
        builder.list.push(value.to_json());
        builder
    }

    pub fn push_list(self, f: &fn(JsonListBuilder) -> JsonListBuilder) -> JsonListBuilder {
        let builder = JsonListBuilder::new();
        self.push(f(builder).unwrap())
    }

    pub fn push_object(self, f: &fn(JsonObjectBuilder) -> JsonObjectBuilder) -> JsonListBuilder {
        let builder = JsonObjectBuilder::new();
        self.push(json::Object(f(builder).unwrap()))
    }
}

pub struct JsonObjectBuilder {
    object: ~json::Object
}

impl JsonObjectBuilder {
    pub fn new() -> JsonObjectBuilder {
        JsonObjectBuilder { object: ~TreeMap::new() }
    }

    pub fn unwrap(self) -> ~json::Object {
        let JsonObjectBuilder { object } = self;
        object
    }

    pub fn insert<T: ToJson>(self, key: ~str, value: T) -> JsonObjectBuilder {
        let mut builder = self;
        builder.object.insert(key, value.to_json());
        builder
    }

    pub fn insert_list<'a>(self, key: ~str, f: &fn(JsonListBuilder) -> JsonListBuilder) -> JsonObjectBuilder {
        let builder = JsonListBuilder::new();
        self.insert(key, f(builder).unwrap())
    }

    pub fn insert_object<'a>(self, key: ~str, f: &fn(JsonObjectBuilder) -> JsonObjectBuilder) -> JsonObjectBuilder {
        let builder = JsonObjectBuilder::new();
        self.insert(key, json::Object(f(builder).unwrap()))
    }
}

/// Transport to talk to Elasticsearch with zeromq
pub struct ZMQTransport { socket: zmq::Socket }

/// Create a zeromq transport to Elasticsearch
impl ZMQTransport {
    pub fn new(ctx: zmq::Context, addr: &str) -> Result<ZMQTransport, zmq::Error> {
        let socket = match ctx.socket(zmq::REQ) {
            Ok(socket) => socket,
            Err(e) => { return Err(e); }
        };

        match socket.connect(addr) {
            Ok(()) => { }
            Err(e) => { return Err(e); }
        }

        Ok(ZMQTransport { socket: socket })
    }

    pub fn send(&self, request: &str) -> Response {
        debug!("request: %s", request);

        match self.socket.send_str(request, 0) {
            Ok(()) => { }
            Err(e) => fail!(e.to_str()),
        }

        match self.socket.recv_msg(0) {
            Ok(msg) => {
                let bytes = msg.to_bytes();
                debug!("response: %s", str::from_bytes(bytes));
                Response::parse(bytes)
            },
            Err(e) => fail!(e.to_str()),
        }
    }
}

/// Zeromq transport implementation
impl Transport for ZMQTransport {
    fn head(&self, path: &str) -> Response { self.send(fmt!("HEAD|%s", path)) }
    fn get(&self, path: &str) -> Response { self.send(fmt!("GET|%s", path)) }
    fn put(&self, path: &str, source: ~json::Object) -> Response {
        self.send(fmt!("PUT|%s|%s", path, json::Object(source).to_str()))
    }
    fn post(&self, path: &str, source: ~json::Object) -> Response {
        self.send(fmt!("POST|%s|%s", path, json::Object(source).to_str()))
    }
    fn delete(&self, path: &str, source: Option<~json::Object>) -> Response {
        match source {
            None => self.send(fmt!("DELETE|%s", path)),
            Some(source) =>
                self.send(fmt!("DELETE|%s|%s",
                    path,
                    json::Object(source).to_str())),
        }
    }
}

/// Helper function to creating a client with zeromq
pub fn connect_with_zmq(ctx: zmq::Context, addr: &str) -> Result<Client, zmq::Error> {
    match ZMQTransport::new(ctx, addr) {
        Ok(transport) => Ok(Client::new(~transport as ~Transport)),
        Err(e) => Err(e),
    }
}

pub struct Response {
    code: uint,
    status: ~str,
    body: json::Json,
}

impl Response {
    fn parse(msg: &[u8]) -> Response {
        let end = msg.len();

        let (start, code) = Response::parse_code(msg, end);
        let (start, status) = Response::parse_status(msg, start, end);
        let body = Response::parse_body(msg, start, end);

        Response { code: code, status: status, body: body }
    }

    fn parse_code(msg: &[u8], end: uint) -> (uint, uint) {
        match msg.slice(0, end).iter().position(|c| *c == '|' as u8) {
            None => fail!("invalid response"),
            Some(i) => {
                match uint::parse_bytes(msg.slice(0u, i), 10u) {
                    Some(code) => (i + 1u, code),
                    None => fail!("invalid status code"),
                }
            }
        }
    }

    fn parse_status(msg: &[u8], start: uint, end: uint) -> (uint, ~str) {
        match msg.slice(start,end).iter().position(|c| *c == '|' as u8) {
            None => fail!("invalid response"),
            Some(i) => (i + 1u, str::from_bytes(msg.slice(start, i))),
        }
    }

    fn parse_body(msg: &[u8], start: uint, end: uint) -> json::Json {
        if start == end { return json::Null; }

        do io::with_bytes_reader(msg.slice(start, end)) |rdr| {
            match json::from_reader(rdr) {
                Ok(json) => json,
                Err(e) => fail!(e.to_str()),
            }
        }
    }
}
