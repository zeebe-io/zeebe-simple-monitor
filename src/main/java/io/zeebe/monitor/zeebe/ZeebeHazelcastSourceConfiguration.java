package io.zeebe.monitor.zeebe;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Import;

import io.zeebe.exporter.source.hazelcast.HazelcastSourceConfiguration;

@ConditionalOnProperty(name="zeebe.exporter.source.hazelcast.enabled", havingValue="true")
@Import(HazelcastSourceConfiguration.class)
public class ZeebeHazelcastSourceConfiguration {
}
