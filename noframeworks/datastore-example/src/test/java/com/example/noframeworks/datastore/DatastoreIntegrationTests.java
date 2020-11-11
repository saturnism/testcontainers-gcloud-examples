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
package com.example.noframeworks.datastore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.example.noframeworks.pubsub.datastore.Person;
import com.example.noframeworks.pubsub.datastore.PersonDao;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DatastoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class DatastoreIntegrationTests {
  private static final String PROJECT_ID = "test-project";

  @Container
  private static final DatastoreEmulatorContainer datastoreEmulator =
      new DatastoreEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"));

  private Datastore datastore;

  @BeforeEach
  void setup() throws IOException {
    DatastoreOptions options =
        DatastoreOptions.newBuilder()
            // host requires "http://" prefix for the emulator
            .setHost("http://" + datastoreEmulator.getEmulatorEndpoint())
            .setCredentials(NoCredentials.getInstance())
            .setRetrySettings(ServiceOptions.getNoRetrySettings())
            .setProjectId("test-project")
            .build();

    this.datastore = options.getService();
  }

  @Test
  void testCrud() {
    PersonDao dao = new PersonDao(datastore);

    Person p = new Person();
    p.setName("Ray");
    Long id = dao.save(p);

    Person retrieved = dao.findById(id);
    assertNotNull(retrieved);
    assertNotNull(retrieved.getId());
    assertEquals(id, retrieved.getId());
    assertEquals("Ray", retrieved.getName());

    dao.delete(retrieved.getId());
    assertNull(dao.findById(retrieved.getId()));
  }
}
