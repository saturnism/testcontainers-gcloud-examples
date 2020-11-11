package com.example.noframeworks.pubsub.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.KeyFactory;

public class PersonDao {
  private final Datastore datastore;
  private final KeyFactory keyFactory;

  public PersonDao(Datastore datastore) {
    this.datastore = datastore;
    this.keyFactory = datastore.newKeyFactory();
  }

  public Long save(Person person) {
    IncompleteKey key = keyFactory.setKind("person").newKey();
    FullEntity<IncompleteKey> entity = Entity.newBuilder(key)
        .set("name", person.getName())
        .build();

    Entity saved = datastore.put(entity);

    return saved.getKey().getId();
  }

  public Person findById(Long id) {
    Entity entity = datastore.get(keyFactory.setKind("person").newKey(id));
    if (entity == null) return null;

    Person person = new Person();
    person.setId(entity.getKey().getId());
    person.setName(entity.getString("name"));
    return person;
  }

  public void delete(Long id) {
    datastore.delete(keyFactory.setKind("person").newKey(id));
  }
}
