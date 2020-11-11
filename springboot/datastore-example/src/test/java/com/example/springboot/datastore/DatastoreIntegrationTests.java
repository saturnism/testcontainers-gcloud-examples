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
package com.example.springboot.datastore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;

// Unlike other integration tests, Spring Cloud GCP supports Datastore emulator without using a container.
// Thus, this integration test actually doesn't use Testcontainers at all!
@SpringBootTest
@ActiveProfiles("test")
public class DatastoreIntegrationTests {
  @TestConfiguration
  static class EmulatorConfiguration {
    // By default, autoconfiguration will initialize application default credentials.
    // For testing purposes, don't use any credentials. Bootstrap w/ NoCredentialsProvider.
    @Bean
    CredentialsProvider googleCredentials() {
      return NoCredentialsProvider.create();
    }
  }

  @Autowired PersonRepsitory personRepsitory;

  @Test
  void testCrud() {
    Person p = new Person();
    p.setName("Ray");
    Person saved = personRepsitory.save(p);
    assertNotNull(saved.getId());

    Person retrieved = personRepsitory.findById(saved.getId()).get();
    assertEquals("Ray", retrieved.getName());

    personRepsitory.delete(retrieved);
    assertFalse(personRepsitory.existsById(saved.getId()));
  }
}
