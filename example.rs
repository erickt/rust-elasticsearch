use std;
use zmq;
use elasticsearch;

import result::{ok, err};
import std::json::to_json;
import std::map::str_hash;
import zmq::{context, to_str};
import elasticsearch::{
    client,
    create_index_builder,
    index_builder,
    search_builder,
    delete_by_query_builder,
    delete_index_builder,
    json_list_builder,
    json_dict_builder
};

fn main() {
    let ctx = alt zmq::init(1) {
      ok(ctx) { ctx }
      err(e) { fail e.to_str(); }
    };

    let client = elasticsearch::connect_with_zmq(ctx, "tcp://localhost:9700");
    io::println(#fmt("%?\n", client.transport.head("/")));
    io::println(#fmt("%?\n", client.transport.get("/")));

    io::println(#fmt("%?\n", client.prepare_create_index("test")
        .set_source(*json_dict_builder()
            .insert_dict("settings", |bld| {
                bld.insert("index.number_of_shards", 1u)
                   .insert("index.number_of_replicas", 0u);
            })
      )
      .execute()));

    io::println(#fmt("%?\n", client.get("test", "test", "1")));

    io::println(#fmt("%?\n", client.prepare_index("test", "test")
      .set_id("1")
      .set_version(2u)
      .set_source(*json_dict_builder()
          .insert("foo", 5.0)
          .insert("bar", "wee")
          .insert_dict("baz", |bld| { bld.insert("a", 2.0); })
          .insert_list("boo", |bld| { bld.push(1.0).push("zzz"); })
      )
      .set_refresh(true)
      .execute()));

    io::println(#fmt("%?\n", client.get("test", "test", "1")));

    io::println(#fmt("%?\n", client.prepare_search()
      .set_indices(~["test"])
      .set_source(*json_dict_builder()
          .insert("fields", ~["foo", "bar"])
      )
      .execute()));

    io::println(#fmt("%?\n", client.delete("test", "test", "1")));

    io::println(#fmt("%?\n", client.prepare_index("test", "test")
      .set_id("2")
      .set_source(*json_dict_builder()
          .insert("bar", "lala")
      )
      .set_refresh(true)
      .execute()));

    io::println(#fmt("%?\n", client.prepare_delete_by_query()
      .set_indices(~["test"])
      .set_source(*json_dict_builder()
          .insert_dict("term", |bld| { bld.insert("bar", "lala"); })
      )
      .execute()));

    io::println(#fmt("%?\n", client.prepare_delete_index()
      .set_indices(~["test"])
      .execute()));
}
