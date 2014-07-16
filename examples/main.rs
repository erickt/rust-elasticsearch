extern crate elasticsearch;

use elasticsearch::JsonObjectBuilder;

fn main() {
    let mut client = elasticsearch::Client::new("http://localhost:9200");
    /*
    println!("{}\n", client.transport.head("/"));
    println!("{}\n", client.transport.get("/"));
    */

    println!("{}\n", client.prepare_create_index("test".to_string())
        .set_source(JsonObjectBuilder::new()
            .insert_object("settings".to_string(), |bld| {
                bld.
                    insert("index.number_of_shards".to_string(), 1u).
                    insert("index.number_of_replicas".to_string(), 0u)
            })
            .unwrap()
        )
        .execute());

    println!("{}\n", client.get("test", "test", "1"));

    println!("{}\n", client.prepare_index("test".to_string(), "test".to_string())
      .set_id("1".to_string())
      .set_version(2u)
      .set_source(JsonObjectBuilder::new()
          .insert("foo".to_string(), 5.0f64)
          .insert("bar".to_string(), "wee".to_string())
          .insert_object("baz".to_string(), |bld| bld.insert("a".to_string(), 2.0f64))
          .insert_list("boo".to_string(), |bld| bld.push("aaa".to_string()).push("zzz".to_string()))
          .unwrap()
      )
      .set_refresh(true)
      .execute());

    println!("{}\n", client.get("test", "test", "1"));

    println!("{}\n", client.prepare_search()
      .set_indices(vec!["test".to_string()])
      .set_source(JsonObjectBuilder::new()
          .insert("fields".to_string(), vec!["foo".to_string(), "bar".to_string()])
          .unwrap()
      )
      .execute());

    println!("{}\n", client.delete("test".to_string(), "test".to_string(), "1".to_string()));

    println!("{}\n", client.prepare_index("test".to_string(), "test".to_string())
      .set_id("2".to_string())
      .set_source(JsonObjectBuilder::new()
          .insert("bar".to_string(), "lala".to_string())
          .unwrap()
      )
      .set_refresh(true)
      .execute());

    println!("{}\n", client.prepare_delete_by_query()
      .set_indices(vec!["test".to_string()])
      .set_source(JsonObjectBuilder::new()
          .insert_object("term".to_string(), |bld| bld.insert("bar".to_string(), "lala".to_string()))
          .unwrap()
      )
      .execute());

    println!("{}\n", client.prepare_delete_index()
      .set_indices(vec!["test".to_string()])
      .execute());
}
