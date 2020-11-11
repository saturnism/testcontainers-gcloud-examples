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
package com.example.noframeworks.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.example.noframeworks.pubsub.spanner.Person;
import com.example.noframeworks.pubsub.spanner.PersonDao;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class SpannerIntegrationTests {
  private static final String PROJECT_ID = "test-project";
  private static final String INSTANCE_ID = "test-instance";
  private static final String DATABASE_ID = "test-database";

  @Container
  private static final SpannerEmulatorContainer spannerEmulator =
      new SpannerEmulatorContainer(
          DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:1.1.1"));

  private static Spanner spanner;
  private static DatabaseClient databaseClient;

  @BeforeAll
  static void setup() throws IOException, ExecutionException, InterruptedException {
    // Configure Spanner client to connect to the emulator
    SpannerOptions options =
        SpannerOptions.newBuilder()
            // Setting emulator host also automatically configures NoCredentials
            .setEmulatorHost(spannerEmulator.getEmulatorGrpcEndpoint())
            .setProjectId("test-project")
            .build();
    spanner = options.getService();

    // Create a Spanner instance
    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    InstanceId instanceId = InstanceId.of(PROJECT_ID, INSTANCE_ID);

    OperationFuture<Instance, CreateInstanceMetadata> instanceFuture =
        instanceAdminClient.createInstance(
            InstanceInfo.newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_ID))
                // make sure to use the special `emulator-config`
                .setInstanceConfigId(InstanceConfigId.of(PROJECT_ID, "emulator-config"))
                .build());
    Instance instance = instanceFuture.get();

    // Read the schema file and create a Spanner Database
    try (InputStream is = SpannerIntegrationTests.class.getResourceAsStream("/schema.ddl")) {
      String ddl = new String(is.readAllBytes());

      // The client doesn't take all of the DDL, and each CREATE statement must be separated into
      // into individual strings, with the ";" statement separator removed...
      // Also, remove any empty strings, otherwise Spanner will complain.
      List<String> parts = Arrays.stream(ddl.split(";")).filter(s -> !s.isBlank())
          .collect(Collectors.toList());

      // Create a new Database
      DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
      OperationFuture<Database, CreateDatabaseMetadata> databaseFuture = databaseAdminClient
          .createDatabase(INSTANCE_ID, DATABASE_ID, parts);
      Database database = databaseFuture.get();

      // Configure Databae Client to be used
      databaseClient = spanner.getDatabaseClient(database.getId());
    }
  }

  @Test
  void testCrud() {
    PersonDao dao = new PersonDao(databaseClient);

    Person p = new Person();
    p.setName("Ray");
    String id = dao.save(p);

    Person retrieved = dao.findById(id);
    assertNotNull(retrieved);
    assertNotNull(retrieved.getId());
    assertEquals(id, retrieved.getId());
    assertEquals("Ray", retrieved.getName());

    dao.delete(retrieved.getId());
    assertNull(dao.findById(retrieved.getId()));
  }
}
