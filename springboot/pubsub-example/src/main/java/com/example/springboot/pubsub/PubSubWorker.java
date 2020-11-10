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
package com.example.springboot.pubsub;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gcp.pubsub.core.subscriber.PubSubSubscriberTemplate;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.springframework.stereotype.Service;

public class PubSubWorker {
  private static final Logger logger = LoggerFactory.getLogger(PubSubWorker.class);

  private final String subscription;
  private final PubSubSubscriberTemplate subscriberTemplate;
  private final Consumer<PubsubMessage> listener;
  private volatile Subscriber subscriber;

  public PubSubWorker(String subscription, PubSubSubscriberTemplate subscriberTemplate) {
    this(subscription, subscriberTemplate, null);
  }

  PubSubWorker(
      String subscription,
      PubSubSubscriberTemplate subscriberTemplate,
      Consumer<PubsubMessage> listener) {
    this.subscription = subscription;
    this.subscriberTemplate = subscriberTemplate;
    this.listener = listener;
  }

  public void start() {
    this.subscriber =
        subscriberTemplate.subscribe(
            subscription,
            (msg) -> {
              msg.ack();

              if (listener != null) {
                listener.accept(msg.getPubsubMessage());
              }
            });
  }

  public void stop() {
    ApiService service = subscriber.stopAsync();
    while (service.isRunning()) {
      service.awaitTerminated();
    }
  }
}
