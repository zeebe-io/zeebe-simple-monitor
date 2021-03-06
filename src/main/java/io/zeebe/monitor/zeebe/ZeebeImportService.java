package io.zeebe.monitor.zeebe;

import com.hazelcast.core.HazelcastInstance;
import io.camunda.zeebe.protocol.Protocol;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.MessageIntent;
import io.camunda.zeebe.protocol.record.intent.MessageStartEventSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.intent.TimerIntent;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.hazelcast.connect.java.ZeebeHazelcast;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.entity.ErrorEntity;
import io.zeebe.monitor.entity.HazelcastConfig;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.entity.JobEntity;
import io.zeebe.monitor.entity.MessageEntity;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.entity.ProcessEntity;
import io.zeebe.monitor.entity.ProcessInstanceEntity;
import io.zeebe.monitor.entity.TimerEntity;
import io.zeebe.monitor.entity.VariableEntity;
import io.zeebe.monitor.repository.ElementInstanceRepository;
import io.zeebe.monitor.repository.ErrorRepository;
import io.zeebe.monitor.repository.HazelcastConfigRepository;
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.JobRepository;
import io.zeebe.monitor.repository.MessageRepository;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import io.zeebe.monitor.repository.ProcessInstanceRepository;
import io.zeebe.monitor.repository.ProcessRepository;
import io.zeebe.monitor.repository.TimerRepository;
import io.zeebe.monitor.repository.VariableRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

@Component
public class ZeebeImportService {

  @Autowired private ProcessRepository processRepository;
  @Autowired private ProcessInstanceRepository processInstanceRepository;
  @Autowired private ElementInstanceRepository elementInstanceRepository;
  @Autowired private VariableRepository variableRepository;
  @Autowired private JobRepository jobRepository;
  @Autowired private IncidentRepository incidentRepository;
  @Autowired private MessageRepository messageRepository;
  @Autowired private MessageSubscriptionRepository messageSubscriptionRepository;
  @Autowired private TimerRepository timerRepository;
  @Autowired private ErrorRepository errorRepository;

  @Autowired private ZeebeNotificationService notificationService;

  @Autowired private HazelcastConfigRepository hazelcastConfigRepository;

  public ZeebeHazelcast importFrom(final HazelcastInstance hazelcast) {

    final var hazelcastConfig =
        hazelcastConfigRepository
            .findById("cfg")
            .orElseGet(
                () -> {
                  final var config = new HazelcastConfig();
                  config.setId("cfg");
                  config.setSequence(-1);
                  return config;
                });

    final var builder =
        ZeebeHazelcast.newBuilder(hazelcast)
            .addProcessListener(
                record -> ifEvent(record, Schema.ProcessRecord::getMetadata, this::importProcess))
            .addProcessInstanceListener(
                record ->
                    ifEvent(
                        record,
                        Schema.ProcessInstanceRecord::getMetadata,
                        this::importProcessInstance))
            .addIncidentListener(
                record -> ifEvent(record, Schema.IncidentRecord::getMetadata, this::importIncident))
            .addJobListener(
                record -> ifEvent(record, Schema.JobRecord::getMetadata, this::importJob))
            .addVariableListener(
                record -> ifEvent(record, Schema.VariableRecord::getMetadata, this::importVariable))
            .addTimerListener(
                record -> ifEvent(record, Schema.TimerRecord::getMetadata, this::importTimer))
            .addMessageListener(
                record -> ifEvent(record, Schema.MessageRecord::getMetadata, this::importMessage))
            .addMessageSubscriptionListener(
                record ->
                    ifEvent(
                        record,
                        Schema.MessageSubscriptionRecord::getMetadata,
                        this::importMessageSubscription))
            .addMessageStartEventSubscriptionListener(
                record ->
                    ifEvent(
                        record,
                        Schema.MessageStartEventSubscriptionRecord::getMetadata,
                        this::importMessageStartEventSubscription))
            .addErrorListener(this::importError)
            .postProcessListener(
                sequence -> {
                  hazelcastConfig.setSequence(sequence);
                  hazelcastConfigRepository.save(hazelcastConfig);
                });

    if (hazelcastConfig.getSequence() >= 0) {
      builder.readFrom(hazelcastConfig.getSequence());
    } else {
      builder.readFromHead();
    }

    return builder.build();
  }

  private <T> void ifEvent(
      final T record,
      final Function<T, Schema.RecordMetadata> extractor,
      final Consumer<T> consumer) {
    final var metadata = extractor.apply(record);
    if (isEvent(metadata)) {
      consumer.accept(record);
    }
  }

  private boolean isEvent(final Schema.RecordMetadata metadata) {
    return metadata.getRecordType() == Schema.RecordMetadata.RecordType.EVENT;
  }

  private void importProcess(final Schema.ProcessRecord record) {
    final int partitionId = record.getMetadata().getPartitionId();

    if (partitionId != Protocol.DEPLOYMENT_PARTITION) {
      // ignore process event on other partitions to avoid duplicates
      return;
    }

    final ProcessEntity entity = new ProcessEntity();
    entity.setKey(record.getProcessDefinitionKey());
    entity.setBpmnProcessId(record.getBpmnProcessId());
    entity.setVersion(record.getVersion());
    entity.setResource(record.getResource().toStringUtf8());
    entity.setTimestamp(record.getMetadata().getTimestamp());
    processRepository.save(entity);
  }

  private void importProcessInstance(final Schema.ProcessInstanceRecord record) {
    if (record.getProcessInstanceKey() == record.getMetadata().getKey()) {
      addOrUpdateProcessInstance(record);
    }

    addElementInstance(record);
  }

  private void addOrUpdateProcessInstance(final Schema.ProcessInstanceRecord record) {

    final Intent intent = ProcessInstanceIntent.valueOf(record.getMetadata().getIntent());
    final long timestamp = record.getMetadata().getTimestamp();
    final long processInstanceKey = record.getProcessInstanceKey();

    final ProcessInstanceEntity entity =
        processInstanceRepository
            .findById(processInstanceKey)
            .orElseGet(
                () -> {
                  final ProcessInstanceEntity newEntity = new ProcessInstanceEntity();
                  newEntity.setPartitionId(record.getMetadata().getPartitionId());
                  newEntity.setKey(processInstanceKey);
                  newEntity.setBpmnProcessId(record.getBpmnProcessId());
                  newEntity.setVersion(record.getVersion());
                  newEntity.setProcessDefinitionKey(record.getProcessDefinitionKey());
                  newEntity.setParentProcessInstanceKey(record.getParentProcessInstanceKey());
                  newEntity.setParentElementInstanceKey(record.getParentElementInstanceKey());
                  return newEntity;
                });

    if (intent == ProcessInstanceIntent.ELEMENT_ACTIVATED) {
      entity.setState("Active");
      entity.setStart(timestamp);
      processInstanceRepository.save(entity);

      notificationService.sendCreatedProcessInstance(
          record.getProcessInstanceKey(), record.getProcessDefinitionKey());

    } else if (intent == ProcessInstanceIntent.ELEMENT_COMPLETED) {
      entity.setState("Completed");
      entity.setEnd(timestamp);
      processInstanceRepository.save(entity);

      notificationService.sendEndedProcessInstance(
          record.getProcessInstanceKey(), record.getProcessDefinitionKey());

    } else if (intent == ProcessInstanceIntent.ELEMENT_TERMINATED) {
      entity.setState("Terminated");
      entity.setEnd(timestamp);
      processInstanceRepository.save(entity);

      notificationService.sendEndedProcessInstance(
          record.getProcessInstanceKey(), record.getProcessDefinitionKey());
    }
  }

  private void addElementInstance(final Schema.ProcessInstanceRecord record) {

    final long position = record.getMetadata().getPosition();
    if (!elementInstanceRepository.existsById(position)) {

      final ElementInstanceEntity entity = new ElementInstanceEntity();
      entity.setPosition(position);
      entity.setPartitionId(record.getMetadata().getPartitionId());
      entity.setKey(record.getMetadata().getKey());
      entity.setIntent(record.getMetadata().getIntent());
      entity.setTimestamp(record.getMetadata().getTimestamp());
      entity.setProcessInstanceKey(record.getProcessInstanceKey());
      entity.setElementId(record.getElementId());
      entity.setFlowScopeKey(record.getFlowScopeKey());
      entity.setProcessDefinitionKey(record.getProcessDefinitionKey());
      entity.setBpmnElementType(record.getBpmnElementType());

      elementInstanceRepository.save(entity);

      notificationService.sendProcessInstanceUpdated(
          record.getProcessInstanceKey(), record.getProcessDefinitionKey());
    }
  }

  private void importIncident(final Schema.IncidentRecord record) {

    final IncidentIntent intent = IncidentIntent.valueOf(record.getMetadata().getIntent());
    final long key = record.getMetadata().getKey();
    final long timestamp = record.getMetadata().getTimestamp();

    final IncidentEntity entity =
        incidentRepository
            .findById(key)
            .orElseGet(
                () -> {
                  final IncidentEntity newEntity = new IncidentEntity();
                  newEntity.setKey(key);
                  newEntity.setBpmnProcessId(record.getBpmnProcessId());
                  newEntity.setProcessDefinitionKey(record.getProcessDefinitionKey());
                  newEntity.setProcessInstanceKey(record.getProcessInstanceKey());
                  newEntity.setElementInstanceKey(record.getElementInstanceKey());
                  newEntity.setJobKey(record.getJobKey());
                  newEntity.setErrorType(record.getErrorType());
                  newEntity.setErrorMessage(record.getErrorMessage());
                  return newEntity;
                });

    if (intent == IncidentIntent.CREATED) {
      entity.setCreated(timestamp);
      incidentRepository.save(entity);

    } else if (intent == IncidentIntent.RESOLVED) {
      entity.setResolved(timestamp);
      incidentRepository.save(entity);
    }
  }

  private void importJob(final Schema.JobRecord record) {

    final JobIntent intent = JobIntent.valueOf(record.getMetadata().getIntent());
    final long key = record.getMetadata().getKey();
    final long timestamp = record.getMetadata().getTimestamp();

    final JobEntity entity =
        jobRepository
            .findById(key)
            .orElseGet(
                () -> {
                  final JobEntity newEntity = new JobEntity();
                  newEntity.setKey(key);
                  newEntity.setProcessInstanceKey(record.getProcessInstanceKey());
                  newEntity.setElementInstanceKey(record.getElementInstanceKey());
                  newEntity.setJobType(record.getType());
                  return newEntity;
                });

    entity.setState(intent.name().toLowerCase());
    entity.setTimestamp(timestamp);
    entity.setWorker(record.getWorker());
    entity.setRetries(record.getRetries());
    jobRepository.save(entity);
  }

  private void importMessage(final Schema.MessageRecord record) {

    final MessageIntent intent = MessageIntent.valueOf(record.getMetadata().getIntent());
    final long key = record.getMetadata().getKey();
    final long timestamp = record.getMetadata().getTimestamp();

    final MessageEntity entity =
        messageRepository
            .findById(key)
            .orElseGet(
                () -> {
                  final MessageEntity newEntity = new MessageEntity();
                  newEntity.setKey(key);
                  newEntity.setName(record.getName());
                  newEntity.setCorrelationKey(record.getCorrelationKey());
                  newEntity.setMessageId(record.getMessageId());
                  newEntity.setPayload(record.getVariables().toString());
                  return newEntity;
                });

    entity.setState(intent.name().toLowerCase());
    entity.setTimestamp(timestamp);
    messageRepository.save(entity);
  }

  private void importMessageSubscription(final Schema.MessageSubscriptionRecord record) {

    final MessageSubscriptionIntent intent =
        MessageSubscriptionIntent.valueOf(record.getMetadata().getIntent());
    final long timestamp = record.getMetadata().getTimestamp();

    final MessageSubscriptionEntity entity =
        messageSubscriptionRepository
            .findByElementInstanceKeyAndMessageName(
                record.getElementInstanceKey(), record.getMessageName())
            .orElseGet(
                () -> {
                  final MessageSubscriptionEntity newEntity = new MessageSubscriptionEntity();
                  newEntity.setId(
                      generateId()); // message subscription doesn't have a key - it is always '-1'
                  newEntity.setElementInstanceKey(record.getElementInstanceKey());
                  newEntity.setMessageName(record.getMessageName());
                  newEntity.setCorrelationKey(record.getCorrelationKey());
                  newEntity.setProcessInstanceKey(record.getProcessInstanceKey());
                  return newEntity;
                });

    entity.setState(intent.name().toLowerCase());
    entity.setTimestamp(timestamp);
    messageSubscriptionRepository.save(entity);
  }

  private void importMessageStartEventSubscription(
      final Schema.MessageStartEventSubscriptionRecord record) {

    final MessageStartEventSubscriptionIntent intent =
        MessageStartEventSubscriptionIntent.valueOf(record.getMetadata().getIntent());
    final long timestamp = record.getMetadata().getTimestamp();

    final MessageSubscriptionEntity entity =
        messageSubscriptionRepository
            .findByProcessDefinitionKeyAndMessageName(
                record.getProcessDefinitionKey(), record.getMessageName())
            .orElseGet(
                () -> {
                  final MessageSubscriptionEntity newEntity = new MessageSubscriptionEntity();
                  newEntity.setId(
                      generateId()); // message subscription doesn't have a key - it is always '-1'
                  newEntity.setMessageName(record.getMessageName());
                  newEntity.setProcessDefinitionKey(record.getProcessDefinitionKey());
                  newEntity.setTargetFlowNodeId(record.getStartEventId());
                  return newEntity;
                });

    entity.setState(intent.name().toLowerCase());
    entity.setTimestamp(timestamp);
    messageSubscriptionRepository.save(entity);
  }

  private void importTimer(final Schema.TimerRecord record) {

    final TimerIntent intent = TimerIntent.valueOf(record.getMetadata().getIntent());
    final long key = record.getMetadata().getKey();
    final long timestamp = record.getMetadata().getTimestamp();

    final TimerEntity entity =
        timerRepository
            .findById(key)
            .orElseGet(
                () -> {
                  final TimerEntity newEntity = new TimerEntity();
                  newEntity.setKey(key);
                  newEntity.setProcessDefinitionKey(record.getProcessDefinitionKey());
                  newEntity.setTargetElementId(record.getTargetElementId());
                  newEntity.setDueDate(record.getDueDate());
                  newEntity.setRepetitions(record.getRepetitions());

                  if (record.getProcessInstanceKey() > 0) {
                    newEntity.setProcessInstanceKey(record.getProcessInstanceKey());
                    newEntity.setElementInstanceKey(record.getElementInstanceKey());
                  }

                  return newEntity;
                });

    entity.setState(intent.name().toLowerCase());
    entity.setTimestamp(timestamp);
    timerRepository.save(entity);
  }

  private void importVariable(final Schema.VariableRecord record) {

    final long position = record.getMetadata().getPosition();
    if (!variableRepository.existsById(position)) {

      final VariableEntity entity = new VariableEntity();
      entity.setPosition(position);
      entity.setTimestamp(record.getMetadata().getTimestamp());
      entity.setProcessInstanceKey(record.getProcessInstanceKey());
      entity.setName(record.getName());
      entity.setValue(record.getValue());
      entity.setScopeKey(record.getScopeKey());
      entity.setState(record.getMetadata().getIntent().toLowerCase());
      variableRepository.save(entity);
    }
  }

  private void importError(final Schema.ErrorRecord record) {

    final var metadata = record.getMetadata();
    final var position = metadata.getPosition();

    final var entity =
        errorRepository
            .findById(position)
            .orElseGet(
                () -> {
                  final var newEntity = new ErrorEntity();
                  newEntity.setPosition(position);
                  newEntity.setErrorEventPosition(record.getErrorEventPosition());
                  newEntity.setProcessInstanceKey(record.getProcessInstanceKey());
                  newEntity.setExceptionMessage(record.getExceptionMessage());
                  newEntity.setStacktrace(record.getStacktrace());
                  newEntity.setTimestamp(metadata.getTimestamp());
                  return newEntity;
                });

    errorRepository.save(entity);
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }
}
