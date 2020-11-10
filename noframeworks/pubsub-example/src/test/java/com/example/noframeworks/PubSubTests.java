package com.example.noframeworks;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class PubSubTests {
  private static final String PROJECT_ID = "test-project";
  private static final String TOPIC_NAME =
      String.format("projects/%s/topics/%s", PROJECT_ID, "test-topic");
  private static final String SUBSCRIPTION_NAME =
      String.format("projects/%s/subscriptions/%s", PROJECT_ID, "test-subscription");

  @Container
  PubSubEmulatorContainer emulator =
      new PubSubEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"));

  ManagedChannel emulatorChannel;
  TransportChannelProvider emulatorChannelProvider;

  /**
   * Bootstrap the emulator with topic and subscriptions.
   *
   * @throws IOException
   */
  @BeforeEach
  void setup() throws IOException {
    this.emulatorChannel =
        ManagedChannelBuilder.forTarget(emulator.getEmulatorEndpoint()).usePlaintext().build();
    this.emulatorChannelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(emulatorChannel));

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

    topicAdminClient.shutdown();
    subscriptionAdminClient.shutdown();
  }

  @Test
  public void testSender() throws IOException, ExecutionException, InterruptedException {
    Publisher publisher =
        Publisher.newBuilder(TOPIC_NAME)
            .setChannelProvider(emulatorChannelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    PubSubSender sender = new PubSubSender(publisher);
    ApiFuture<String> future = sender.send("hello!");
    String id = future.get();

    publisher.shutdown();

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
                      .setMaxMessages(10)
                      .setSubscription(SUBSCRIPTION_NAME)
                      .build());

      assertEquals(1, response.getReceivedMessagesCount());
      assertEquals(id, response.getReceivedMessages(0).getMessage().getMessageId());

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
  public void testWorker() throws IOException, ExecutionException, InterruptedException {
    Publisher publisher =
        Publisher.newBuilder(TOPIC_NAME)
            .setChannelProvider(emulatorChannelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    PubSubSender sender = new PubSubSender(publisher);
    ApiFuture<String> future = sender.send("hello!");
    String id = future.get();

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

    await().until(() -> ids.size(), equalTo(1));
    assertEquals(id, ids.get(0));

    worker.stop();
  }

  @AfterEach
  public void teardown() throws IOException {
    // Drain all messages
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

      if (response.getReceivedMessagesCount() > 0) {
        Builder ackBuilder = AcknowledgeRequest.newBuilder();
        ackBuilder.setSubscription(SUBSCRIPTION_NAME);
        for (ReceivedMessage msg : response.getReceivedMessagesList()) {
          ackBuilder.addAckIds(msg.getAckId());
        }
        subscriberStub.acknowledgeCallable().call(ackBuilder.build());
      }
    }

    // Shut down the channel
    emulatorChannel.shutdown();
    while (!emulatorChannel.isTerminated()) {
      try {
        emulatorChannel.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // do nothing
      }
    }
  }
}
