/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe.monitor;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.web.config.EnableSpringDataWebSupport;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import io.zeebe.monitor.zeebe.ZeebeHazelcastSourceConfiguration;
import io.zeebe.monitor.zeebe.ZeebeKafkaJsonSourceConfiguration;
import io.zeebe.monitor.zeebe.ZeebeKafkaProtobufSourceConfiguration;
import io.zeebe.spring.client.EnableZeebeClient;

@SpringBootApplication
@EnableZeebeClient
@EnableScheduling
@EnableAsync
@EnableSpringDataWebSupport
@Import({ZeebeHazelcastSourceConfiguration.class, ZeebeKafkaJsonSourceConfiguration.class, ZeebeKafkaProtobufSourceConfiguration.class})
public class ZeebeSimpleMonitorApp {

  public static void main(String... args) {
    SpringApplication.run(ZeebeSimpleMonitorApp.class, args);
  }

  @Bean
  public ScheduledExecutorService scheduledExecutor() {
    return Executors.newSingleThreadScheduledExecutor();
  }

  @Bean
  public Executor asyncExecutor() {
    final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(1);
    executor.setMaxPoolSize(1);
    executor.setQueueCapacity(32);
    executor.initialize();
    return executor;
  }
}
