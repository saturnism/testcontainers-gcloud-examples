package com.example.noframeworks.pubsub.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import java.util.Arrays;
import java.util.UUID;

public class PersonDao {
  private final DatabaseClient databaseClient;

  public PersonDao(DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  public String save(Person person) {
    String id = UUID.randomUUID().toString();

    databaseClient
        .readWriteTransaction()
        .run(
            (tx) -> {
              tx.buffer(
                  Mutation.newInsertBuilder("people")
                      .set("id")
                      .to(id)
                      .set("name")
                      .to(person.getName())
                      .build());
              return null;
            });

    return id;
  }

  public Person findById(String id) {
    ResultSet rs =
        databaseClient
            .singleUse()
            .read("people", KeySet.singleKey(Key.of(id)), Arrays.asList("id", "name"));

    if (!rs.next()) return null;

    Person p = new Person();

    p.setId(rs.getString(0));
    p.setName(rs.getString(1));

    return p;
  }

  public void delete(String id) {
    databaseClient
        .readWriteTransaction()
        .run(
            (tx) -> {
              tx.buffer(Mutation.delete("people", Key.of(id)));
              return null;
            });
  }
}
