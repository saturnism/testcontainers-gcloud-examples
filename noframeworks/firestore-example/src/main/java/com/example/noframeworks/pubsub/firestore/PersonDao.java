package com.example.noframeworks.pubsub.firestore;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import java.util.concurrent.ExecutionException;

public class PersonDao {
  private final Firestore firestore;

  public PersonDao(Firestore firestore) {
    this.firestore = firestore;
  }

  public String save(Person person) throws ExecutionException, InterruptedException {
    return firestore.collection("people").add(person).get().getId();
  }

  public Person findById(String id) throws ExecutionException, InterruptedException {
    DocumentReference ref = firestore.collection("people").document(id);
    DocumentSnapshot snapshot = ref.get().get();
    if (!snapshot.exists()) return null;

    return snapshot.toObject(Person.class);
  }

  public void delete(String id) throws ExecutionException, InterruptedException {
    firestore.collection("people").document(id).delete().get();
  }
}
