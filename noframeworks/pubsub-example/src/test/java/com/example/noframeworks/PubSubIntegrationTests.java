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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.AcknowledgeRequest.Builder;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class PubSubIntegrationTests {
  private static final String PROJECT_ID = "test-project";
  private static final String TOPIC_NAME =
      String.format("projects/%s/topics/%s", PROJECT_ID, "test-topic");
  private static final String SUBSCRIPTION_NAME =
      String.format("projects/%s/subscriptions/%s", PROJECT_ID, "test-subscription");

  @Container
  private static final PubSubEmulatorContainer pubsubEmulator =
      new PubSubEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"));

  private static ManagedChannel emulatorChannel;
  private static TransportChannelProvider emulatorChannelProvider;

  /**
   * Bootstrap the emulator with topic and subscriptions.
   *
   * @throws IOException
   */
  @BeforeAll
  static void setup() throws IOException {
    // Create a gRPC channel that connects to the emulator host/port.
    // Production connections are over HTTPS, but emulator connection is plaintext.
    emulatorChannel =
        ManagedChannelBuilder.forTarget(pubsubEmulator.getEmulatorEndpoint()).usePlaintext().build();

    // Create a channel provider that holds the channel. Channel Provider is used everywhere else.
    emulatorChannelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(emulatorChannel));

    // When connecting, in addition to the emulator channel provider, also use
    // NoCredentialsProvider.
    // Otherwise, your application default credentials will be sent and it may not be accepted
    // by the emulator. Moreover, the connection is over plaintext - don't send real credentials.
    TopicAdminClient topicAdminClient =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(emulatorChannelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build());
    topicAdminClient.createTopic(TOPIC_NAME);

    SubscriptionAdminClient subscriptionAdminClient =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(emulatorChannelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build());
    subscriptionAdminClient.createSubscription(
        SUBSCRIPTION_NAME, TOPIC_NAME, PushConfig.newBuilder().build(), 10);

    // Always shutdown the clients when you are done.
    // In some cases, you have to wait for shutdown to complete.
    topicAdminClient.shutdown();
    subscriptionAdminClient.shutdown();
    emulatorChannel.shutdown();
  }

  @Test
  void testSender() throws IOException, ExecutionException, InterruptedException {
    // Create a Publisher that uses the emulator channel provider.
    Publisher publisher =
        Publisher.newBuilder(TOPIC_NAME)
            .setChannelProvider(emulatorChannelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    PubSubSender sender = new PubSubSender(publisher);
    ApiFuture<String> future = sender.send("hello!");
    String id = future.get();

    // Always shutdown gracefully.
    publisher.shutdown();

    // Create a subscriber to verify that the message was sent and received by this subscriber.
    try (GrpcSubscriberStub subscriberStub =
        GrpcSubscriberStub.create(
            SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(emulatorChannelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build())) {

      // Use simple pull mechanism to see what's in the subscription.
      PullResponse response =
          subscriberStub
              .pullCallable()
              .call(
                  PullRequest.newBuilder()
                      // pull more than you send, just in case there are other issues.
                      .setMaxMessages(10)
                      .setSubscription(SUBSCRIPTION_NAME)
                      .build());

      // Assert you've received 1 message with the correct information.
      assertEquals(1, response.getReceivedMessagesCount());
      assertEquals("hello!", response.getReceivedMessages(0).getMessage().getData().toStringUtf8());
      assertEquals(id, response.getReceivedMessages(0).getMessage().getMessageId());

      // Acknowledge the message, otherwise it'll still be in the subscription and it can
      // mess up subsequent tests.
      subscriberStub
          .acknowledgeCallable()
          .call(
              AcknowledgeRequest.newBuilder()
                  .setSubscription(SUBSCRIPTION_NAME)
                  .addAckIds(response.getReceivedMessages(0).getAckId())
                  .build());
    }
  }

  @Test
  void testWorker() throws IOException, ExecutionException, InterruptedException {
    Publisher publisher =
        Publisher.newBuilder(TOPIC_NAME)
            .setChannelProvider(emulatorChannelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    PubSubSender sender = new PubSubSender(publisher);
    ApiFuture<String> future = sender.send("hello!");
    String id = future.get();
    publisher.shutdown();

    List<String> ids = Collections.synchronizedList(new LinkedList<>());

    PubSubWorker worker =
        new PubSubWorker(
            SUBSCRIPTION_NAME,
            (msg) -> {
              ids.add(msg.getMessageId());
            },
            emulatorChannelProvider,
            NoCredentialsProvider.create());
    worker.start();

    // Use awaitility to wait until at least a message has been received.
    await().until(() -> ids.size(), greaterThan(0));

    // Assert the content of the message
    assertEquals(1, ids.size());
    assertEquals(id, ids.get(0));

    worker.stop();
  }

  @AfterEach
  void teardown() throws IOException {
    // In case there are issues with the test, make sure all unprocessed messages are drained.
    // So that those messages don't get processed in subsequent tests.
    try (GrpcSubscriberStub subscriberStub =
        GrpcSubscriberStub.create(
            SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(emulatorChannelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build())) {

      PullResponse response =
          subscriberStub
              .pullCallable()
              .call(
                  PullRequest.newBuilder()
                      .setMaxMessages(1000)
                      .setSubscription(SUBSCRIPTION_NAME)
                      .build());

      // Ack all remanining messages.
      if (response.getReceivedMessagesCount() > 0) {
        Builder ackBuilder = AcknowledgeRequest.newBuilder();
        ackBuilder.setSubscription(SUBSCRIPTION_NAME);
        for (ReceivedMessage msg : response.getReceivedMessagesList()) {
          ackBuilder.addAckIds(msg.getAckId());
        }
        subscriberStub.acknowledgeCallable().call(ackBuilder.build());
      }
    }
  }
}
