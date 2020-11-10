/*
 * Copyright 2020 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
