extern mod std;
extern mod zmq;
extern mod elasticsearch;

use elasticsearch::{JsonListBuilder, JsonObjectBuilder};

fn main() {
    let ctx = match zmq::init(1) {
      Ok(ctx) => ctx,
      Err(e) => fail e.to_str(),
    };

    let client = elasticsearch::connect_with_zmq(ctx, "tcp://localhost:9700");
    io::println(fmt!("%?\n", client.transport.head("/")));
    io::println(fmt!("%?\n", client.transport.get("/")));

    io::println(fmt!("%?\n", client.prepare_create_index(~"test")
        .set_source(JsonObjectBuilder::new()
            .insert_object(~"settings", |bld| {
                bld.insert(~"index.number_of_shards", 1u)
                   .insert(~"index.number_of_replicas", 0u);
            })
            .consume()
      )
      .execute()));

    io::println(fmt!("%?\n", client.get("test", "test", "1")));

    io::println(fmt!("%?\n", client.prepare_index(~"test", ~"test")
      .set_id(~"1")
      .set_version(2u)
      .set_source(JsonObjectBuilder::new()
          .insert(~"foo", 5.0)
          .insert(~"bar", ~"wee")
          .insert_object(~"baz", |bld| { bld.insert(~"a", 2.0); })
          .insert_list(~"boo", |bld| { bld.push(~"aaa").push(~"zzz"); })
          .consume()
      )
      .set_refresh(true)
      .execute()));

    io::println(fmt!("%?\n", client.get("test", "test", "1")));

    io::println(fmt!("%?\n", client.prepare_search()
      .set_indices(~[~"test"])
      .set_source(JsonObjectBuilder::new()
          .insert(~"fields", ~[~"foo", ~"bar"])
          .consume()
      )
      .execute()));

    io::println(fmt!("%?\n", client.delete(~"test", ~"test", ~"1")));

    io::println(fmt!("%?\n", client.prepare_index(~"test", ~"test")
      .set_id(~"2")
      .set_source(JsonObjectBuilder::new()
          .insert(~"bar", ~"lala")
          .consume()
      )
      .set_refresh(true)
      .execute()));

    io::println(fmt!("%?\n", client.prepare_delete_by_query()
      .set_indices(~[~"test"])
      .set_source(JsonObjectBuilder::new()
          .insert_object(~"term", |bld| { bld.insert(~"bar", ~"lala"); })
          .consume()
      )
      .execute()));

    io::println(fmt!("%?\n", client.prepare_delete_index()
      .set_indices(~[~"test"])
      .execute()));
}
