package com.example.noframeworks;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;

public class PubSubSender {
  private final Publisher publisher;

  public PubSubSender(String topicName) throws IOException {
    this.publisher = Publisher.newBuilder(topicName).build();
  }

  PubSubSender(Publisher publisher) {
    this.publisher = publisher;
  }

  public ApiFuture<String> send(String message) {
    return publisher.publish(
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).build());
  }
}
