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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.core.publisher.PubSubPublisherTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class PubSubSender {
  private final String topicName;
  private final PubSubPublisherTemplate publisherTemplate;

  public PubSubSender(
      @Value("${app.topic-name}") String topicName, PubSubPublisherTemplate publisherTemplate) {
    this.publisherTemplate = publisherTemplate;

    Assert.hasText(topicName, "topicName cannot be null");
    this.topicName = topicName;
  }

  public ListenableFuture<String> send(String message) {
    return publisherTemplate.publish(topicName, message);
  }
}
