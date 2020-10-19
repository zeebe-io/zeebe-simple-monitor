package io.zeebe.monitor.zeebe;

import com.hazelcast.core.HazelcastInstance;

import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.source.ProtobufSource;
import io.zeebe.exporter.source.ProtobufSourceConnector;
import io.zeebe.exporter.source.hazelcast.HazelcastSource;
import io.zeebe.exporter.source.hazelcast.HazelcastSourceConnector;
import io.zeebe.hazelcast.connect.java.ZeebeHazelcast;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.entity.HazelcastConfig;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.entity.JobEntity;
import io.zeebe.monitor.entity.MessageEntity;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.entity.TimerEntity;
import io.zeebe.monitor.entity.VariableEntity;
import io.zeebe.monitor.entity.WorkflowEntity;
import io.zeebe.monitor.entity.WorkflowInstanceEntity;
import io.zeebe.monitor.repository.ElementInstanceRepository;
import io.zeebe.monitor.repository.HazelcastConfigRepository;
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.JobRepository;
import io.zeebe.monitor.repository.MessageRepository;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import io.zeebe.monitor.repository.TimerRepository;
import io.zeebe.monitor.repository.VariableRepository;
import io.zeebe.monitor.repository.WorkflowInstanceRepository;
import io.zeebe.monitor.repository.WorkflowRepository;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.MessageIntent;
import io.zeebe.protocol.record.intent.MessageStartEventSubscriptionIntent;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.record.intent.TimerIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

@Component
public class ZeebeImportService implements ProtobufSourceConnector, HazelcastSourceConnector {

  @Autowired private WorkflowRepository workflowRepository;
  @Autowired private WorkflowInstanceRepository workflowInstanceRepository;
  @Autowired private ElementInstanceRepository elementInstanceRepository;
  @Autowired private VariableRepository variableRepository;
  @Autowired private JobRepository jobRepository;
  @Autowired private IncidentRepository incidentRepository;
  @Autowired private MessageRepository messageRepository;
  @Autowired private MessageSubscriptionRepository messageSubscriptionRepository;
  @Autowired private TimerRepository timerRepository;

  @Autowired private ZeebeNotificationService notificationService;

  @Autowired private HazelcastConfigRepository hazelcastConfigRepository;

  private HazelcastConfig getHazelcastConfig() {
      
    return hazelcastConfigRepository
      .findById("cfg")
      .orElseGet(
        () -> {
          final var config = new HazelcastConfig();
          config.setId("cfg");
          config.setSequence(-1);
          return config;
        });
  }

  public void connectTo(ProtobufSource source) {
    source.addDeploymentListener(
      record ->
          withKey(record, Schema.DeploymentRecord::getMetadata, this::importDeployment));
    source.addWorkflowInstanceListener(
      record ->
          withKey(
              record,
              Schema.WorkflowInstanceRecord::getMetadata,
              this::importWorkflowInstance));
              source.addIncidentListener(
      record -> withKey(record, Schema.IncidentRecord::getMetadata, this::importIncident));
      source.addJobListener(
      record -> withKey(record, Schema.JobRecord::getMetadata, this::importJob));
      source.addVariableListener(
      record -> withKey(record, Schema.VariableRecord::getMetadata, this::importVariable));
      source.addTimerListener(
      record -> withKey(record, Schema.TimerRecord::getMetadata, this::importTimer));
      source.addMessageListener(
      record -> withKey(record, Schema.MessageRecord::getMetadata, this::importMessage));
      source.addMessageSubscriptionListener(this::importMessageSubscription);
    source.addMessageStartEventSubscriptionListener(this::importMessageStartEventSubscription);
  }
    
  public void connectTo(HazelcastSource source) {

    source.postProcessListener(
      sequence -> {
        var hazelcastConfig = getHazelcastConfig();
        hazelcastConfig.setSequence(sequence);
        hazelcastConfigRepository.save(hazelcastConfig);
      });
  }

  @Override
  public Optional<Long> startPosition() {
    var hazelcastConfig = getHazelcastConfig();

    if (hazelcastConfig.getSequence() >= 0) {
      return Optional.of(hazelcastConfig.getSequence());
    } else {
      return Optional.empty();
    }

  }

  private <T> void withKey(
      T record, Function<T, Schema.RecordMetadata> extractor, Consumer<T> consumer) {
    final var metadata = extractor.apply(record);
    if (!hasKey(metadata)) {
      consumer.accept(record);
    }
  }

  private boolean hasKey(Schema.RecordMetadata metadata) {
    return metadata.getKey() < 0;
  }

  private void importDeployment(final Schema.DeploymentRecord record) {

    final DeploymentIntent intent = DeploymentIntent.valueOf(record.getMetadata().getIntent());
    final int partitionId = record.getMetadata().getPartitionId();

    if (intent != DeploymentIntent.CREATED || partitionId != Protocol.DEPLOYMENT_PARTITION) {
      // ignore deployment event on other partitions to avoid duplicates
      return;
    }

    record
        .getResourcesList()
        .forEach(
            resource -> {
              record.getDeployedWorkflowsList().stream()
                  .filter(w -> w.getResourceName().equals(resource.getResourceName()))
                  .forEach(
                      deployedWorkflow -> {
                        final WorkflowEntity entity = new WorkflowEntity();
                        entity.setKey(deployedWorkflow.getWorkflowKey());
                        entity.setBpmnProcessId(deployedWorkflow.getBpmnProcessId());
                        entity.setVersion(deployedWorkflow.getVersion());
                        entity.setResource(resource.getResource().toStringUtf8());
                        entity.setTimestamp(record.getMetadata().getTimestamp());
                        workflowRepository.save(entity);
                      });
            });
  }

  private void importWorkflowInstance(final Schema.WorkflowInstanceRecord record) {
    if (record.getWorkflowInstanceKey() == record.getMetadata().getKey()) {
      addOrUpdateWorkflowInstance(record);
    }

    addElementInstance(record);
  }

  private void addOrUpdateWorkflowInstance(final Schema.WorkflowInstanceRecord record) {

    final Intent intent = WorkflowInstanceIntent.valueOf(record.getMetadata().getIntent());
    final long timestamp = record.getMetadata().getTimestamp();
    final long workflowInstanceKey = record.getWorkflowInstanceKey();

    final WorkflowInstanceEntity entity =
        workflowInstanceRepository
            .findById(workflowInstanceKey)
            .orElseGet(
                () -> {
                  final WorkflowInstanceEntity newEntity = new WorkflowInstanceEntity();
                  newEntity.setPartitionId(record.getMetadata().getPartitionId());
                  newEntity.setKey(workflowInstanceKey);
                  newEntity.setBpmnProcessId(record.getBpmnProcessId());
                  newEntity.setVersion(record.getVersion());
                  newEntity.setWorkflowKey(record.getWorkflowKey());
                  newEntity.setParentWorkflowInstanceKey(record.getParentWorkflowInstanceKey());
                  newEntity.setParentElementInstanceKey(record.getParentElementInstanceKey());
                  return newEntity;
                });

    if (intent == WorkflowInstanceIntent.ELEMENT_ACTIVATED) {
      entity.setState("Active");
      entity.setStart(timestamp);
      workflowInstanceRepository.save(entity);

      notificationService.sendCreatedWorkflowInstance(
          record.getWorkflowInstanceKey(), record.getWorkflowKey());

    } else if (intent == WorkflowInstanceIntent.ELEMENT_COMPLETED) {
      entity.setState("Completed");
      entity.setEnd(timestamp);
      workflowInstanceRepository.save(entity);

      notificationService.sendEndedWorkflowInstance(
          record.getWorkflowInstanceKey(), record.getWorkflowKey());

    } else if (intent == WorkflowInstanceIntent.ELEMENT_TERMINATED) {
      entity.setState("Terminated");
      entity.setEnd(timestamp);
      workflowInstanceRepository.save(entity);

      notificationService.sendEndedWorkflowInstance(
          record.getWorkflowInstanceKey(), record.getWorkflowKey());
    }
  }

  private void addElementInstance(final Schema.WorkflowInstanceRecord record) {

    final long position = record.getMetadata().getPosition();
    if (!elementInstanceRepository.existsById(position)) {

      final ElementInstanceEntity entity = new ElementInstanceEntity();
      entity.setPosition(position);
      entity.setPartitionId(record.getMetadata().getPartitionId());
      entity.setKey(record.getMetadata().getKey());
      entity.setIntent(record.getMetadata().getIntent());
      entity.setTimestamp(record.getMetadata().getTimestamp());
      entity.setWorkflowInstanceKey(record.getWorkflowInstanceKey());
      entity.setElementId(record.getElementId());
      entity.setFlowScopeKey(record.getFlowScopeKey());
      entity.setWorkflowKey(record.getWorkflowKey());
      entity.setBpmnElementType(record.getBpmnElementType().name());

      elementInstanceRepository.save(entity);

      notificationService.sendWorkflowInstanceUpdated(
          record.getWorkflowInstanceKey(), record.getWorkflowKey());
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
                  newEntity.setWorkflowKey(record.getWorkflowKey());
                  newEntity.setWorkflowInstanceKey(record.getWorkflowInstanceKey());
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
                  newEntity.setWorkflowInstanceKey(record.getWorkflowInstanceKey());
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
                  newEntity.setWorkflowInstanceKey(record.getWorkflowInstanceKey());
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
            .findByWorkflowKeyAndMessageName(record.getWorkflowKey(), record.getMessageName())
            .orElseGet(
                () -> {
                  final MessageSubscriptionEntity newEntity = new MessageSubscriptionEntity();
                  newEntity.setId(
                      generateId()); // message subscription doesn't have a key - it is always '-1'
                  newEntity.setMessageName(record.getMessageName());
                  newEntity.setWorkflowKey(record.getWorkflowKey());
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
                  newEntity.setWorkflowKey(record.getWorkflowKey());
                  newEntity.setTargetFlowNodeId(record.getTargetFlowNodeId());
                  newEntity.setDueDate(record.getDueDate());
                  newEntity.setRepetitions(record.getRepetitions());

                  if (record.getWorkflowInstanceKey() > 0) {
                    newEntity.setWorkflowInstanceKey(record.getWorkflowInstanceKey());
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
      entity.setWorkflowInstanceKey(record.getWorkflowInstanceKey());
      entity.setName(record.getName());
      entity.setValue(record.getValue());
      entity.setScopeKey(record.getScopeKey());
      entity.setState(record.getMetadata().getIntent().toLowerCase());
      variableRepository.save(entity);
    }
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }
}
