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

import com.example.springboot.pubsub.PubsubServiceIntegrationTests.Initializer;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.gcp.autoconfigure.core.GcpContextAutoConfiguration;
import org.springframework.cloud.gcp.autoconfigure.pubsub.GcpPubSubAutoConfiguration;
import org.springframework.cloud.gcp.autoconfigure.pubsub.GcpPubSubEmulatorAutoConfiguration;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.subscriber.PubSubSubscriberTemplate;
import org.springframework.cloud.gcp.pubsub.support.AcknowledgeablePubsubMessage;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.concurrent.ListenableFuture;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@ContextConfiguration(initializers = Initializer.class)
public class PubsubServiceIntegrationTests {
  @Container
  private static final PubSubEmulatorContainer pubsubEmulator =
      new PubSubEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"));

  // Pub/Sub emulator topics/subscription must be bootstrapped before Spring Boot starts.
  // The easiest way to do that is to use an Initializer. This is even before @BeforeAll.
  static class Initializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext ctx) {
      // First, use PubSubAdmin to bootstrap the topic and subscriptions.
      ApplicationContextRunner runner =
          new ApplicationContextRunner()
              // By default, autoconfiguration will initialize application default credentials.
              // For testing purposes, don't use any credentials. Bootstrap w/ NoCredentailsProvider.
              .withBean(
                  "googleCredentials",
                  CredentialsProvider.class,
                  () -> NoCredentialsProvider.create())
              // Use a couple of autoconfigurations to bootstrap PubSubAdmin bean.
              .withConfiguration(
                  AutoConfigurations.of(
                      GcpContextAutoConfiguration.class,
                      GcpPubSubAutoConfiguration.class,
                      GcpPubSubEmulatorAutoConfiguration.class))
              // Make sure the PubSubAdmin bean is using the emulator host and test project ID
              .withPropertyValues(
                  "spring.cloud.gcp.project-id=test-project",
                  "spring.cloud.gcp.pubsub.emulator-host=" + pubsubEmulator.getEmulatorEndpoint())
              .run(
                  runnerCtx -> {
                    PubSubAdmin admin = runnerCtx.getBean(PubSubAdmin.class);

                    admin.createTopic("test-topic");
                    admin.createSubscription("test-subscription", "test-topic");

                    admin.close();
                  });

      TestPropertyValues.of(
              "spring.cloud.gcp.pubsub.emulator-host=" + pubsubEmulator.getEmulatorEndpoint())
          .applyTo(ctx);
    }
  }

  // By default, autoconfiguration will initialize application default credentials.
  // For testing purposes, don't use any credentials. Bootstrap w/ NoCredentailsProvider.
  @TestConfiguration
  static class PubSubEmulatorConfiguration {
    @Bean
    CredentialsProvider googleCredentials() {
      return NoCredentialsProvider.create();
    }
  }

  @Autowired PubsubService pubsubService;
  @Autowired PubSubSubscriberTemplate subscriberTemplate;

  @Test
  void testSend() throws ExecutionException, InterruptedException {
    ListenableFuture<String> future = pubsubService.send("hello!");
    String id = future.get();

    List<AcknowledgeablePubsubMessage> msgs =
        await().until(() -> subscriberTemplate.pull("test-subscription", 10, true), not(empty()));

    assertEquals(1, msgs.size());
    assertEquals(id, msgs.get(0).getPubsubMessage().getMessageId());
    assertEquals("hello!", msgs.get(0).getPubsubMessage().getData().toStringUtf8());

    for (AcknowledgeablePubsubMessage msg : msgs) {
      msg.ack();
    }
  }

  @AfterEach
  void teardown() {
    // Drain any messages that are still in the subscription so that they don't interfere with subsequent tests.
    await()
        .until(
            () -> subscriberTemplate.pullAndAck("test-subscription", 1000, true),
            hasSize(0));
  }
}
