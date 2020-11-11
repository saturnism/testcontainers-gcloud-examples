package com.example.noframeworks.pubsub.firestore;

import com.google.cloud.firestore.annotation.DocumentId;

public class Person {
  @DocumentId
  private String id;
  private String name;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
