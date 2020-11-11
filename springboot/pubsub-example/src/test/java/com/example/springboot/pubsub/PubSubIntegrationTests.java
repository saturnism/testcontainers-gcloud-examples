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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.publisher.PubSubPublisherTemplate;
import org.springframework.cloud.gcp.pubsub.core.subscriber.PubSubSubscriberTemplate;
import org.springframework.cloud.gcp.pubsub.support.AcknowledgeablePubsubMessage;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.concurrent.ListenableFuture;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class PubSubIntegrationTests {
  @Container
  private static final PubSubEmulatorContainer pubsubEmulator =
      new PubSubEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"));

  @DynamicPropertySource
  static void emulatorProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.cloud.gcp.pubsub.emulator-host", pubsubEmulator::getEmulatorEndpoint);
  }

  @BeforeAll
  static void setup() throws Exception {
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("dns:///" + pubsubEmulator.getEmulatorEndpoint())
            .usePlaintext()
            .build();
    TransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

    TopicAdminClient topicAdminClient =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(channelProvider)
                .build());

    SubscriptionAdminClient subscriptionAdminClient =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build());

    PubSubAdmin admin =
        new PubSubAdmin(() -> "test-project", topicAdminClient, subscriptionAdminClient);

    admin.createTopic("test-topic");
    admin.createSubscription("test-subscription", "test-topic");

    admin.close();
    channel.shutdown();
  }

  // By default, autoconfiguration will initialize application default credentials.
  // For testing purposes, don't use any credentials. Bootstrap w/ NoCredentialsProvider.
  @TestConfiguration
  static class PubSubEmulatorConfiguration {
    @Bean
    CredentialsProvider googleCredentials() {
      return NoCredentialsProvider.create();
    }
  }

  @Autowired PubSubSender sender;

  @Autowired PubSubSubscriberTemplate subscriberTemplate;
  @Autowired PubSubPublisherTemplate publisherTemplate;

  @Test
  void testSend() throws ExecutionException, InterruptedException {
    ListenableFuture<String> future = sender.send("hello!");

    List<AcknowledgeablePubsubMessage> msgs =
        await().until(() -> subscriberTemplate.pull("test-subscription", 10, true), not(empty()));

    assertEquals(1, msgs.size());
    assertEquals(future.get(), msgs.get(0).getPubsubMessage().getMessageId());
    assertEquals("hello!", msgs.get(0).getPubsubMessage().getData().toStringUtf8());

    for (AcknowledgeablePubsubMessage msg : msgs) {
      msg.ack();
    }
  }

  @Test
  void testWorker() throws ExecutionException, InterruptedException {
    ListenableFuture<String> future = publisherTemplate.publish("test-topic", "hi!");

    List<PubsubMessage> messages = Collections.synchronizedList(new LinkedList<>());
    PubSubWorker worker =
        new PubSubWorker(
            "test-subscription",
            subscriberTemplate,
            (msg) -> {
              messages.add(msg);
            });
    worker.start();

    await().until(() -> messages, not(empty()));
    assertEquals(1, messages.size());
    assertEquals(future.get(), messages.get(0).getMessageId());
    assertEquals("hi!", messages.get(0).getData().toStringUtf8());

    worker.stop();
  }

  @AfterEach
  void teardown() {
    // Drain any messages that are still in the subscription so that they don't interfere with
    // subsequent tests.
    await().until(() -> subscriberTemplate.pullAndAck("test-subscription", 1000, true), hasSize(0));
  }
}
