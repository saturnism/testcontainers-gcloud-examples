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
package com.example.springboot.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.InstanceNotFoundException;
import com.google.cloud.spanner.Spanner;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.gcp.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import org.springframework.cloud.gcp.data.spanner.core.admin.SpannerSchemaUtils;
import org.springframework.cloud.gcp.data.spanner.core.mapping.SpannerMappingContext;
import org.springframework.cloud.gcp.data.spanner.core.mapping.SpannerPersistentEntity;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class SpannerIntegrationTests {
  private static final Logger logger = LoggerFactory.getLogger(SpannerIntegrationTests.class);

  private static final String PROJECT_ID = "test-project";
  private static final String INSTANCE_ID = "test-instance";

  @Container
  private static final SpannerEmulatorContainer spannerEmulator =
      new SpannerEmulatorContainer(
          DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:1.1.1"));

  @DynamicPropertySource
  static void emulatorProperties(DynamicPropertyRegistry registry) {
    registry.add(
        "spring.cloud.gcp.spanner.emulator-host", spannerEmulator::getEmulatorGrpcEndpoint);
  }

  @TestConfiguration
  static class EmulatorConfiguration {
    // By default, autoconfiguration will initialize application default credentials.
    // For testing purposes, don't use any credentials. Bootstrap w/ NoCredentialsProvider.
    @Bean
    CredentialsProvider googleCredentials() {
      return NoCredentialsProvider.create();
    }
  }

  @Autowired Spanner spanner;
  @Autowired SpannerDatabaseAdminTemplate spannerAdmin;
  @Autowired SpannerSchemaUtils spannerSchemaUtils;
  @Autowired PersonRepsitory personRepsitory;
  @Autowired SpannerMappingContext spannerMappingContext;

  @Before
  void setup() throws ExecutionException, InterruptedException {
    // Create an instance
    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    InstanceId instanceId = InstanceId.of(PROJECT_ID, INSTANCE_ID);

    // InstanceAdminClient already prefixes (unintuitively) the instance ID w/
    // `projects/{projectId}`.
    try {
      instanceAdminClient.getInstance(instanceId.getInstance());
    } catch (InstanceNotFoundException e) {
      // If instance doesn't exist, create a new Spanner instance in the emulator
      OperationFuture<Instance, CreateInstanceMetadata> operationFuture =
          instanceAdminClient.createInstance(
              InstanceInfo.newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_ID))
                  // make sure to use the special `emulator-config`
                  .setInstanceConfigId(InstanceConfigId.of(PROJECT_ID, "emulator-config"))
                  .build());
      operationFuture.get();
    }

    // Create the tables
    SpannerPersistentEntity<?> persistentEntity =
        spannerMappingContext.getPersistentEntity(Person.class);
    if (!spannerAdmin.tableExists(persistentEntity.tableName())) {
      spannerAdmin.executeDdlStrings(
          Arrays.asList(spannerSchemaUtils.getCreateTableDdlString(Person.class)),
          !spannerAdmin.databaseExists());
    }
  }

  @Test
  void testCrud() {
    Person p = new Person();
    p.setId(UUID.randomUUID().toString());
    p.setName("Ray");
    Person saved = personRepsitory.save(p);

    Person retrieved = personRepsitory.findById(saved.getId()).get();
    assertEquals("Ray", retrieved.getName());

    personRepsitory.delete(retrieved);
    assertFalse(personRepsitory.existsById(saved.getId()));
  }
}
