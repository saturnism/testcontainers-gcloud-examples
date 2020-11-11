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
package com.example.noframeworks.firestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.example.noframeworks.pubsub.firestore.Person;
import com.example.noframeworks.pubsub.firestore.PersonDao;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class DatastoreIntegrationTests {
  private static final String PROJECT_ID = "test-project";

  @Container
  private static final FirestoreEmulatorContainer firestoreEmulator =
      new FirestoreEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"));

  public static class FakeCredentials extends Credentials {
    private final Map<String, List<String>> HEADERS =
        ImmutableMap.of("Authorization", Arrays.asList("Bearer owner"));

    @Override
    public String getAuthenticationType() {
      throw new IllegalArgumentException("Not supported");
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) {
      return HEADERS;
    }

    @Override
    public boolean hasRequestMetadata() {
      return true;
    }

    @Override
    public boolean hasRequestMetadataOnly() {
      return true;
    }

    @Override
    public void refresh() {}
  }

  private Firestore firestore;

  @BeforeEach
  void setup() throws IOException {
    FirestoreOptions options =
        FirestoreOptions.newBuilder()
            .setProjectId(PROJECT_ID)
            .setCredentialsProvider(FixedCredentialsProvider.create(new FakeCredentials()))
            .setChannelProvider(
                InstantiatingGrpcChannelProvider.newBuilder()
                    .setEndpoint(firestoreEmulator.getEmulatorEndpoint())
                    .setChannelConfigurator(input -> input.usePlaintext())
                    .build())
            .build();

    this.firestore = options.getService();
  }

  @Test
  void testCrud() throws ExecutionException, InterruptedException {
    PersonDao dao = new PersonDao(firestore);

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
