#![crate_name = "elasticsearch"]

#![feature(phase)]

#[phase(plugin, link)]
extern crate log;

extern crate serialize;
extern crate url;
extern crate http;

use std::collections::TreeMap;

use http::client::RequestWriter;
use http::headers::content_type;
use http::method;

use serialize::json::{Json, ToJson};
use serialize::json;

/// The high level interface to elasticsearch
pub struct Client {
    addr: String,
}

impl Client {
    /// Create an elasticsearch client
    pub fn new(addr: &str) -> Client {
        Client {
            addr: addr.to_string(),
        }
    }

    /// Create an index
    pub fn prepare_create_index<'a>(&'a mut self, index: String) -> CreateIndexBuilder<'a> {
        CreateIndexBuilder::new(self, index)
    }

    /// Delete indices
    pub fn prepare_delete_index<'a>(&'a mut self) -> DeleteIndexBuilder<'a> {
        DeleteIndexBuilder::new(self)
    }

    /// Get a specific document
    pub fn get(&mut self, index: &str, typ: &str, id: &str) -> Response {
        let path = [
            url::encode_component(index),
            url::encode_component(typ),
            url::encode_component(id)
        ].connect("/");

        self.request(method::Get, path.as_slice(), None)
    }

    /// Create an index builder that will create documents
    pub fn prepare_index<'a>(&'a mut self, index: String, typ: String) -> IndexBuilder<'a> {
        IndexBuilder::new(self, index, typ)
    }

    /// Create a search builder that will query elasticsearch
    pub fn prepare_search<'a>(&'a mut self) -> SearchBuilder<'a> {
        SearchBuilder::new(self)
    }

    /// Delete a document
    pub fn delete(&mut self, index: String, typ: String, id: String) -> Response {
        self.prepare_delete(index, typ, id).execute()
    }

    /// Delete a document
    pub fn prepare_delete<'a>(&'a mut self, index: String, typ: String, id: String) -> DeleteBuilder<'a> {
        DeleteBuilder::new(self, index, typ, id)
    }

    /// Create a search builder that will query elasticsearch
    pub fn prepare_delete_by_query<'a>(&'a mut self) -> DeleteByQueryBuilder<'a> {
        DeleteByQueryBuilder::new(self)
    }

    fn request(&mut self, method: method::Method, request: &str, body: Option<json::Object>) -> Response {
        debug!("request: {} {} {} {}", self.addr, method, request, body);

        let url = format!("{}/{}", self.addr, request);
        let url = from_str(url.as_slice()).unwrap();
        let mut request: RequestWriter = RequestWriter::new(method, url).unwrap();

        match body {
            Some(body) => {
                let body = json::Object(body).to_string();
                request.headers.content_length = Some(body.len());
                request.headers.content_type = Some(
                    content_type::MediaType::new(
                        "application".to_string(),
                        "json".to_string(),
                        vec!()
                    )
                );
                request.write(body.as_bytes()).unwrap();
            }
            None => { }
        }

        let mut response = match request.read_response() {
            Ok(response) => response,
            Err(_) => { fail!() }
        };

        Response {
            code: response.status.code(),
            status: response.status.reason().to_string(),
            body: json::from_reader(&mut response).unwrap(),
        }
    }
}

pub enum Consistency { One, Quorum, All }
pub enum Replication { Sync, Async }
pub enum OpType { CREATE, INDEX }
pub enum VersionType { INTERNAL, EXTERNAL }

pub struct CreateIndexBuilder<'a> {
    client: &'a mut Client,
    index: String,
    timeout: Option<String>,
    source: Option<json::Object>,
}

impl<'a> CreateIndexBuilder<'a> {
    pub fn new(client: &'a mut Client, index: String) -> CreateIndexBuilder<'a> {
        CreateIndexBuilder {
            client: client,
            index: index,
            timeout: None,
            source: None,
        }
    }

    pub fn set_timeout(self, timeout: String) -> CreateIndexBuilder<'a> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_source(self, source: json::Object) -> CreateIndexBuilder<'a> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }
    pub fn execute(self) -> Response {
        let CreateIndexBuilder {
            client,
            index,
            timeout,
            source,
        } = self;

        let mut path = url::encode_component(index.as_slice());

        let mut params = vec!();

        match timeout {
            None => { },
            Some(ref s) => {
                params.push(format!("timeout={}", s));
            }
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&").as_slice());
        }

        let source = match source {
            None => TreeMap::new(),
            Some(source) => source,
        };

        client.request(method::Put, path.as_slice(), Some(source))
    }
}

pub struct DeleteIndexBuilder<'a> {
    client: &'a mut Client,
    indices: Vec<String>,
    timeout: Option<String>,
}

impl<'a> DeleteIndexBuilder<'a> {
    pub fn new(client: &'a mut Client) -> DeleteIndexBuilder<'a> {
        DeleteIndexBuilder {
            client: client,
            indices: vec!(),
            timeout: None,
        }
    }

    pub fn set_indices(self, indices: Vec<String>) -> DeleteIndexBuilder<'a> {
        let mut builder = self;
        builder.indices = indices;
        builder
    }
    pub fn set_timeout(self, timeout: String) -> DeleteIndexBuilder<'a> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn execute(self) -> Response {
        let DeleteIndexBuilder {
            client,
            indices,
            timeout,
        } = self;

        let indices: Vec<String> = indices.iter().map(|i| {
            url::encode_component(i.as_slice())
        }).collect();
        let mut path = indices.connect(",");

        // Build the query parameters.
        let mut params = vec!();

        match timeout {
            None => { },
            Some(ref timeout) => params.push(format!("timeout={}", timeout)),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&").as_slice());
        }

        client.request(method::Delete, path.as_slice(), None)
    }
}

pub struct IndexBuilder<'a> {
    client: &'a mut Client,
    index: String,
    typ: String,
    id: Option<String>,

    consistency: Option<Consistency>,
    op_type: OpType,
    parent: Option<String>,
    percolate: Option<String>,
    refresh: bool,
    replication: Option<Replication>,
    routing: Option<String>,
    timeout: Option<String>,
    timestamp: Option<String>,
    ttl: Option<String>,
    version: Option<uint>,
    version_type: VersionType,

    source: Option<json::Object>,
}

impl<'a> IndexBuilder<'a> {
    pub fn new(client: &'a mut Client, index: String, typ: String) -> IndexBuilder<'a> {
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

    pub fn set_id(self, id: String) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.id = Some(id);
        builder
    }
    pub fn set_consistency(self, consistency: Consistency) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.consistency = Some(consistency);
        builder
    }
    pub fn set_op_type(self, op_type: OpType) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.op_type = op_type;
        builder
    }
    pub fn set_parent(self, parent: String) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.parent = Some(parent);
        builder
    }
    pub fn set_percolate(self, percolate: String) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.percolate = Some(percolate);
        builder
    }
    pub fn set_refresh(self, refresh: bool) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.refresh = refresh;
        builder
    }
    pub fn set_replication(self, replication: Replication) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.replication = Some(replication);
        builder
    }
    pub fn set_routing(self, routing: String) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_timeout(self, timeout: String) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_timestamp(self, timestamp: String) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.timestamp = Some(timestamp);
        builder
    }
    pub fn set_ttl(self, ttl: String) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.ttl = Some(ttl);
        builder
    }
    pub fn set_version(self, version: uint) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.version = Some(version);
        builder
    }
    pub fn set_version_type(self, version_type: VersionType) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.version_type = version_type;
        builder
    }
    pub fn set_source(self, source: json::Object) -> IndexBuilder<'a> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }
    pub fn execute(self) -> Response {
        let IndexBuilder {
            client,
            index,
            typ,
            id,
            consistency,
            op_type,
            parent,
            percolate,
            refresh,
            replication,
            routing,
            timeout,
            timestamp,
            ttl,
            version,
            version_type,
            source,
        } = self;

        let mut path = vec!(
            url::encode_component(index.as_slice()),
            url::encode_component(typ.as_slice()),
        );

        // FIXME: https://github.com/mozilla/rust/issues/2549
        match id {
            None => { },
            Some(ref id) => path.push(url::encode_component(id.as_slice())),
        }

        let mut path = path.connect("/");
        let mut params = vec!();

        match consistency {
            None => { },
            Some(One) => params.push("consistency=one".to_string()),
            Some(Quorum) => params.push("consistency=quorum".to_string()),
            Some(All) => params.push("consistency=all".to_string()),
        }

        match op_type {
            CREATE => params.push("op_type=create".to_string()),
            INDEX => { }
        }

        match parent {
            None => { },
            Some(ref s) => params.push(format!("parent={}", s)),
        }

        match percolate {
            None => { }
            Some(ref s) =>  params.push(format!("percolate={}", s)),
        }

        if refresh { params.push("refresh=true".to_string()); }

        match replication {
            None => { },
            Some(Sync) => params.push("replication=sync".to_string()),
            Some(Async) => params.push("replication=async".to_string()),
        }

        match routing {
            None => { },
            Some(ref s) => params.push(format!("routing={}", s)),
        }

        match timeout {
            None => { },
            Some(ref s) => params.push(format!("timeout={}", s)),
        }

        match timestamp {
            None => { },
            Some(ref s) => params.push(format!("timestamp={}", s)),
        }

        match ttl {
            None => { },
            Some(ref s) => params.push(format!("ttl={}", s)),
        }

        match version {
            None => { },
            Some(ref i) => { params.push(format!("version={}", i)); }
        }

        match version_type {
            INTERNAL => { },
            EXTERNAL => params.push("version_type=external".to_string()),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&").as_slice());
        }

        let source = match source {
            None => TreeMap::new(),
            Some(source) => source,
        };

        match id {
            None => client.request(method::Post, path.as_slice(), Some(source)),
            Some(_) => client.request(method::Put, path.as_slice(), Some(source)),
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

pub struct SearchBuilder<'a> {
    client: &'a mut Client,
    indices: Vec<String>,
    types: Vec<String>,

    preference: Option<String>,
    routing: Option<String>,
    scroll: Option<String>,
    search_type: Option<SearchType>,
    timeout: Option<String>,

    source: Option<json::Object>
}

impl<'a> SearchBuilder<'a> {
    pub fn new(client: &'a mut Client) -> SearchBuilder<'a> {
        SearchBuilder {
            client: client,
            indices: vec!(),
            types: vec!(),

            preference: None,
            routing: None,
            scroll: None,
            search_type: None,
            timeout: None,

            source: None,
        }
    }

    pub fn set_indices(self, indices: Vec<String>) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.indices = indices;
        builder
    }
    pub fn set_types(self, types: Vec<String>) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.types = types;
        builder
    }
    pub fn set_preference(self, preference: String) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.preference = Some(preference);
        builder
    }
    pub fn set_routing(self, routing: String) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_scroll(self, scroll: String) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.scroll = Some(scroll);
        builder
    }
    pub fn set_search_type(self, search_type: SearchType) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.search_type = Some(search_type);
        builder
    }
    pub fn set_timeout(self, timeout: String) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_source(self, source: json::Object) -> SearchBuilder<'a> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }
    pub fn execute(self) -> Response {
        let SearchBuilder {
            client,
            indices,
            types,
            preference,
            routing,
            scroll,
            search_type,
            timeout,
            source,
        } = self;

        let indices: Vec<String> = indices.iter().map(|i| {
            url::encode_component(i.as_slice())
        }).collect();

        let types: Vec<String> = types.iter().map(|t| {
            url::encode_component(t.as_slice())
        }).collect();

        let mut path = vec!();

        if indices.is_empty() {
            path.push("_all".to_string());
        } else {
            path.push(indices.connect(","));
        }

        if !types.is_empty() {
            path.push(types.connect(","));
        }

        path.push("_search".to_string());

        let mut path = path.connect("/");

        // Build the query parameters.
        let mut params = vec!();

        match preference {
            None => { },
            Some(ref s) => params.push(format!("preference={}", s)),
        }

        match routing {
            None => { },
            Some(ref s) => params.push(format!("routing={}", s)),
        }

        match scroll {
            None => { },
            Some(ref s) => params.push(format!("scroll={}", s)),
        }

        match search_type {
            None => { },
            Some(DfsQueryThenFetch) =>
                params.push("search_type=dfs_query_then_fetch".to_string()),
            Some(QueryThenFetch) =>
                params.push("search_type=query_then_fetch".to_string()),
            Some(DfsQueryAndFetch) =>
                params.push("search_type=dfs_query_and_fetch".to_string()),
            Some(QueryAndFetch) =>
                params.push("search_type=query_and_fetch".to_string()),
            Some(Scan) => params.push("search_type=scan".to_string()),
            Some(Count) => params.push("search_type=count".to_string()),
        }

        match timeout {
            None => { }
            Some(ref s) => params.push(format!("timeout={}", s)),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&").as_slice());
        }

        client.request(method::Post, path.as_slice(), source)
    }
}

pub struct DeleteBuilder<'a> {
    client: &'a mut Client,
    index: String,
    typ: String,
    id: String,

    consistency: Option<Consistency>,
    refresh: bool,
    replication: Option<Replication>,
    routing: Option<String>,
    timeout: Option<String>,
    version: Option<uint>,
    version_type: VersionType,
}

impl<'a> DeleteBuilder<'a> {
    pub fn new(
        client: &'a mut Client,
        index: String,
        typ: String,
        id: String
    ) -> DeleteBuilder<'a> {
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

    pub fn set_consistency(self, consistency: Consistency) -> DeleteBuilder<'a> {
        let mut builder = self;
        builder.consistency = Some(consistency);
        builder
    }
    pub fn set_parent(self, parent: String) -> DeleteBuilder<'a> {
        // We use the parent for routing.
        let mut builder = self;
        builder.routing = Some(parent);
        builder
    }
    pub fn set_refresh(self, refresh: bool) -> DeleteBuilder<'a> {
        let mut builder = self;
        builder.refresh = refresh;
        builder
    }
    pub fn set_replication(self, replication: Replication) -> DeleteBuilder<'a> {
        let mut builder = self;
        builder.replication = Some(replication);
        builder
    }
    pub fn set_routing(self, routing: String) -> DeleteBuilder<'a> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_timeout(self, timeout: String) -> DeleteBuilder<'a> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_version(self, version: uint) -> DeleteBuilder<'a> {
        let mut builder = self;
        builder.version = Some(version);
        builder
    }
    pub fn set_version_type(self, version_type: VersionType) -> DeleteBuilder<'a> {
        let mut builder = self;
        builder.version_type = version_type;
        builder
    }
    pub fn execute(self) -> Response {
        let DeleteBuilder {
            client,
            index,
            typ,
            id,
            consistency,
            refresh,
            replication,
            routing,
            timeout,
            version,
            version_type,
        } = self;

        let mut path = [
            url::encode_component(index.as_slice()),
            url::encode_component(typ.as_slice()),
            url::encode_component(id.as_slice())
        ].connect("/");

        // Build the query parameters.
        let mut params = vec!();

        match consistency {
            None => { },
            Some(One) => params.push("consistency=one".to_string()),
            Some(Quorum) => params.push("consistency=quorum".to_string()),
            Some(All) => params.push("consistency=all".to_string()),
        }

        if refresh { params.push("refresh=true".to_string()); }

        match replication {
            None => { }
            Some(Sync) => params.push("replication=sync".to_string()),
            Some(Async) => params.push("replication=async".to_string()),
        }

        match routing {
            None => { }
            Some(ref s) => params.push(format!("routing={}", *s)),
        }

        match timeout {
            None => { }
            Some(ref s) => params.push(format!("timeout={}", *s)),
        }

        match version {
            None => { }
            Some(ref s) => params.push(format!("version={}", *s)),
        }

        match version_type {
            INTERNAL => { }
            EXTERNAL => params.push("version_type=external".to_string()),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&").as_slice());
        }

        client.request(method::Delete, path.as_slice(), None)
    }
}

pub struct DeleteByQueryBuilder<'a> {
    client: &'a mut Client,
    indices: Vec<String>,
    types: Vec<String>,

    consistency: Option<Consistency>,
    refresh: bool,
    replication: Option<Replication>,
    routing: Option<String>,
    timeout: Option<String>,

    source: Option<json::Object>,
}

impl<'a> DeleteByQueryBuilder<'a> {
    pub fn new(client: &'a mut Client) -> DeleteByQueryBuilder<'a> {
        DeleteByQueryBuilder {
            client: client,
            indices: vec!(),
            types: vec!(),

            consistency: None,
            refresh: false,
            replication: None,
            routing: None,
            timeout: None,

            source: None,
        }
    }

    pub fn set_indices(self, indices: Vec<String>) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.indices = indices;
        builder
    }
    pub fn set_types(self, types: Vec<String>) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.types = types;
        builder
    }
    pub fn set_consistency(self, consistency: Consistency) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.consistency = Some(consistency);
        builder
    }
    pub fn set_refresh(self, refresh: bool) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.refresh = refresh;
        builder
    }
    pub fn set_replication(self, replication: Replication) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.replication = Some(replication);
        builder
    }
    pub fn set_routing(self, routing: String) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.routing = Some(routing);
        builder
    }
    pub fn set_timeout(self, timeout: String) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.timeout = Some(timeout);
        builder
    }
    pub fn set_source(self, source: json::Object) -> DeleteByQueryBuilder<'a> {
        let mut builder = self;
        builder.source = Some(source);
        builder
    }

    pub fn execute(self) -> Response {
        let DeleteByQueryBuilder {
            client,
            indices,
            types,
            consistency,
            refresh,
            replication,
            routing,
            timeout,
            source,
        } = self;

        let mut path = vec!();

        if indices.is_empty() {
            path.push("_all".to_string());
        } else {
            path.push(indices.connect(","));
        }

        if !types.is_empty() {
            path.push(types.connect(","));
        }

        path.push("_query".to_string());

        let mut path = path.connect("/");

        // Build the query parameters.
        let mut params = vec!();

        match consistency {
            None => {}
            Some(One) => params.push("consistency=one".to_string()),
            Some(Quorum) => params.push("consistency=quorum".to_string()),
            Some(All) => params.push("consistency=all".to_string()),
        }

        if refresh { params.push("refresh=true".to_string()); }

        match replication {
            None => { }
            Some(Sync)  => params.push("replication=sync".to_string()),
            Some(Async) => params.push("replication=async".to_string()),
        }

        match routing {
            None => { }
            Some(ref routing) => params.push(format!("routing={}", routing)),
        }

        match timeout {
            None => { }
            Some(ref timeout) => params.push(format!("timeout={}", timeout)),
        }

        if !params.is_empty() {
            path.push_str("?");
            path.push_str(params.connect("&").as_slice());
        }

        client.request(method::Delete, path.as_slice(), source)
    }
}

pub struct JsonListBuilder {
    list: Vec<Json>
}

impl JsonListBuilder {
    pub fn new() -> JsonListBuilder {
        JsonListBuilder { list: vec!() }
    }

    pub fn unwrap(self) -> Vec<Json> {
        let JsonListBuilder { list } = self;
        list
    }

    pub fn push<T: ToJson>(self, value: T) -> JsonListBuilder {
        let mut builder = self;
        builder.list.push(value.to_json());
        builder
    }

    pub fn push_list(self, f: |JsonListBuilder| -> JsonListBuilder) -> JsonListBuilder {
        let builder = JsonListBuilder::new();
        self.push(f(builder).unwrap())
    }

    pub fn push_object(self, f: |JsonObjectBuilder| -> JsonObjectBuilder) -> JsonListBuilder {
        let builder = JsonObjectBuilder::new();
        self.push(json::Object(f(builder).unwrap()))
    }
}

pub struct JsonObjectBuilder {
    object: json::Object
}

impl JsonObjectBuilder {
    pub fn new() -> JsonObjectBuilder {
        JsonObjectBuilder { object: TreeMap::new() }
    }

    pub fn unwrap(self) -> json::Object {
        let JsonObjectBuilder { object } = self;
        object
    }

    pub fn insert<T: ToJson>(self, key: String, value: T) -> JsonObjectBuilder {
        let mut builder = self;
        builder.object.insert(key, value.to_json());
        builder
    }

    pub fn insert_list<'a>(self, key: String, f: |JsonListBuilder| -> JsonListBuilder) -> JsonObjectBuilder {
        let builder = JsonListBuilder::new();
        self.insert(key, f(builder).unwrap())
    }

    pub fn insert_object<'a>(self, key: String, f: |JsonObjectBuilder| -> JsonObjectBuilder) -> JsonObjectBuilder {
        let builder = JsonObjectBuilder::new();
        self.insert(key, json::Object(f(builder).unwrap()))
    }
}

#[deriving(PartialEq, Clone, Show)]
pub struct Response {
    pub code: u16,
    pub status: String,
    pub body: json::Json,
}
