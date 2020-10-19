package io.zeebe.monitor.zeebe;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Import;

import io.zeebe.exporter.source.kafka.KafkaProtobufSourceConfiguration;

@ConditionalOnExpression("'${zeebe.exporter.source.kafka.enabled}' == 'true' && '${zeebe.exporter.source.kafka.format}' == 'protobuf'")
@Import(KafkaProtobufSourceConfiguration.class)
public class ZeebeKafkaProtobufSourceConfiguration {
}
