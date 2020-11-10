package com.example.noframeworks;

import com.google.api.core.ApiService;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubWorker {
  private static final Logger logger = LoggerFactory.getLogger(PubSubWorker.class);
  private final Subscriber subscriber;
  private final Consumer<PubsubMessage> listener;

  public PubSubWorker(String subscriptionName) {
    this.listener = null;
    this.subscriber =
        Subscriber.newBuilder(
                subscriptionName,
                (msg, reply) -> {
                  process(msg, reply);
                })
            .build();
  }

  PubSubWorker(String subscriptionName,
      Consumer<PubsubMessage> listener,
      TransportChannelProvider channelProvider,
      CredentialsProvider credentialsProvider) {
    this.listener = listener;
    this.subscriber =
        Subscriber.newBuilder(
                subscriptionName,
                (msg, reply) -> {
                  process(msg, reply);
                })
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
  }

  protected void process(PubsubMessage msg, AckReplyConsumer reply) {
    logger.info("Received: " + msg.getData().toStringUtf8());

    reply.ack();

    if (listener != null) {
      listener.accept(msg);
    }
  }

  public void start() {
    subscriber.startAsync();
  }

  public void stop() {
    ApiService service = subscriber.stopAsync();
    while (service.isRunning()) {
      service.awaitTerminated();
    }
  }
}
