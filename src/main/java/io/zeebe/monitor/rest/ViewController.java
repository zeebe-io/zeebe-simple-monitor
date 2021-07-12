package io.zeebe.monitor.rest;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.CatchEvent;
import io.camunda.zeebe.model.bpmn.instance.ErrorEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.FlowElement;
import io.camunda.zeebe.model.bpmn.instance.SequenceFlow;
import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.instance.TimerEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.entity.ElementInstanceStatistics;
import io.zeebe.monitor.entity.ErrorEntity;
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
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.JobRepository;
import io.zeebe.monitor.repository.MessageRepository;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import io.zeebe.monitor.repository.ProcessInstanceRepository;
import io.zeebe.monitor.repository.ProcessRepository;
import io.zeebe.monitor.repository.TimerRepository;
import io.zeebe.monitor.repository.VariableRepository;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import javax.transaction.Transactional;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@Controller
public class ViewController {

  private static final int FIRST_PAGE = 0;
  private static final int PAGE_RANGE = 2;

  private static final List<String> PROCESS_INSTANCE_ENTERED_INTENTS =
      Arrays.asList("ELEMENT_ACTIVATED");
  private static final List<String> PROCESS_INSTANCE_COMPLETED_INTENTS =
      Arrays.asList("ELEMENT_COMPLETED", "ELEMENT_TERMINATED");
  private static final List<String> EXCLUDE_ELEMENT_TYPES =
      Arrays.asList(BpmnElementType.MULTI_INSTANCE_BODY.name());
  private static final List<String> JOB_COMPLETED_INTENTS = Arrays.asList("completed", "canceled");

  private final String basePath;
  private final String logoPath;
  private final String customCssPath;
  private final String customJsPath;
  private final String customTitle;

  @Autowired private ProcessRepository processRepository;
  @Autowired private ProcessInstanceRepository processInstanceRepository;
  @Autowired private ElementInstanceRepository activityInstanceRepository;
  @Autowired private IncidentRepository incidentRepository;
  @Autowired private JobRepository jobRepository;
  @Autowired private MessageRepository messageRepository;
  @Autowired private MessageSubscriptionRepository messageSubscriptionRepository;
  @Autowired private TimerRepository timerRepository;
  @Autowired private VariableRepository variableRepository;
  @Autowired private ErrorRepository errorRepository;

  public ViewController(@Value("${server.servlet.context-path}") final String basePath,
                        @Value("${white-label.logo.path}") final String logoPath,
                        @Value("${white-label.custom.title}") final String customTitle,
                        @Value("${white-label.custom.css.path}") final String customCssPath,
                        @Value("${white-label.custom.js.path}") final String customJsPath){
    this.basePath = basePath.endsWith("/") ? basePath : basePath + "/";
    this.logoPath = logoPath;
    this.customTitle = customTitle;
    this.customCssPath = customCssPath;
    this.customJsPath = customJsPath;
  }

  @GetMapping("/")
  public String index(final Map<String, Object> model, final Pageable pageable) {
    addCommonVariablesToModel(model);
    return processList(model, pageable);
  }

  @GetMapping("/views/processes")
  public String processList(final Map<String, Object> model, final Pageable pageable) {

    final long count = processRepository.count();

    final List<ProcessDto> processes = new ArrayList<>();
    for (final ProcessEntity processEntity : processRepository.findAll(pageable)) {
      final ProcessDto dto = toDto(processEntity);
      processes.add(dto);
    }

    model.put("processes", processes);
    model.put("count", count);

    addCommonVariablesToModel(model);
    addPaginationToModel(model, pageable, count);

    return "process-list-view";
  }

  private ProcessDto toDto(final ProcessEntity processEntity) {
    final long processDefinitionKey = processEntity.getKey();

    final long running =
        processInstanceRepository.countByProcessDefinitionKeyAndEndIsNull(processDefinitionKey);
    final long ended =
        processInstanceRepository.countByProcessDefinitionKeyAndEndIsNotNull(processDefinitionKey);

    final ProcessDto dto = ProcessDto.from(processEntity, running, ended);
    return dto;
  }

  @GetMapping("/views/processes/{key}")
  @Transactional
  public String processDetail(
      @PathVariable final long key, final Map<String, Object> model, final Pageable pageable) {

    final ProcessEntity process =
        processRepository
            .findByKey(key)
            .orElseThrow(() -> new RuntimeException("No process found with key: " + key));

    model.put("process", toDto(process));
    model.put("resource", getProcessResource(process));

    final List<ElementInstanceState> elementInstanceStates = getElementInstanceStates(key);
    model.put("instance.elementInstances", elementInstanceStates);

    final long count = processInstanceRepository.countByProcessDefinitionKey(key);

    final List<ProcessInstanceListDto> instances = new ArrayList<>();
    for (final ProcessInstanceEntity instanceEntity :
        processInstanceRepository.findByProcessDefinitionKey(key, pageable)) {
      instances.add(toDto(instanceEntity));
    }

    model.put("instances", instances);
    model.put("count", count);

    final List<TimerDto> timers =
        timerRepository.findByProcessDefinitionKeyAndProcessInstanceKeyIsNull(key).stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    model.put("timers", timers);

    final List<MessageSubscriptionDto> messageSubscriptions =
        messageSubscriptionRepository
            .findByProcessDefinitionKeyAndProcessInstanceKeyIsNull(key)
            .stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    model.put("messageSubscriptions", messageSubscriptions);

    final var resourceAsStream = new ByteArrayInputStream(process.getResource().getBytes());
    final var bpmn = Bpmn.readModelFromStream(resourceAsStream);
    model.put("instance.bpmnElementInfos", getBpmnElementInfos(bpmn));

    addCommonVariablesToModel(model);
    addPaginationToModel(model, pageable, count);

    return "process-detail-view";
  }

  private List<ElementInstanceState> getElementInstanceStates(final long key) {

    final List<ElementInstanceStatistics> elementEnteredStatistics =
        processRepository.getElementInstanceStatisticsByKeyAndIntentIn(
            key, PROCESS_INSTANCE_ENTERED_INTENTS, EXCLUDE_ELEMENT_TYPES);

    final Map<String, Long> elementCompletedCount =
        processRepository
            .getElementInstanceStatisticsByKeyAndIntentIn(
                key, PROCESS_INSTANCE_COMPLETED_INTENTS, EXCLUDE_ELEMENT_TYPES)
            .stream()
            .collect(
                Collectors.toMap(
                    ElementInstanceStatistics::getElementId, ElementInstanceStatistics::getCount));

    final List<ElementInstanceState> elementInstanceStates =
        elementEnteredStatistics.stream()
            .map(
                s -> {
                  final ElementInstanceState state = new ElementInstanceState();

                  final String elementId = s.getElementId();
                  state.setElementId(elementId);

                  final long completedInstances = elementCompletedCount.getOrDefault(elementId, 0L);
                  final long enteredInstances = s.getCount();

                  state.setActiveInstances(enteredInstances - completedInstances);
                  state.setEndedInstances(completedInstances);

                  return state;
                })
            .collect(Collectors.toList());
    return elementInstanceStates;
  }

  private ProcessInstanceListDto toDto(final ProcessInstanceEntity instance) {

    final ProcessInstanceListDto dto = new ProcessInstanceListDto();
    dto.setProcessInstanceKey(instance.getKey());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setProcessDefinitionKey(instance.getProcessDefinitionKey());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(instance.getState());

    dto.setStartTime(Instant.ofEpochMilli(instance.getStart()).toString());

    if (isEnded) {
      dto.setEndTime(Instant.ofEpochMilli(instance.getEnd()).toString());
    }

    return dto;
  }

  @GetMapping("/views/instances")
  public String instanceList(final Map<String, Object> model, final Pageable pageable) {

    final long count = processInstanceRepository.count();

    final List<ProcessInstanceListDto> instances = new ArrayList<>();
    for (final ProcessInstanceEntity instanceEntity : processInstanceRepository.findAll(pageable)) {
      final ProcessInstanceListDto dto = toDto(instanceEntity);
      instances.add(dto);
    }

    model.put("instances", instances);
    model.put("count", count);

    addCommonVariablesToModel(model);
    addPaginationToModel(model, pageable, count);

    return "instance-list-view";
  }

  @GetMapping("/views/instances/{key}")
  @Transactional
  public String instanceDetail(
      @PathVariable final long key, final Map<String, Object> model, final Pageable pageable) {

    final ProcessInstanceEntity instance =
        processInstanceRepository
            .findByKey(key)
            .orElseThrow(() -> new RuntimeException("No process instance found with key: " + key));

    processRepository
        .findByKey(instance.getProcessDefinitionKey())
        .ifPresent(process -> model.put("resource", getProcessResource(process)));

    model.put("instance", toInstanceDto(instance));

    addCommonVariablesToModel(model);

    return "instance-detail-view";
  }

  private ProcessInstanceDto toInstanceDto(final ProcessInstanceEntity instance) {
    final List<ElementInstanceEntity> events =
        StreamSupport.stream(
                activityInstanceRepository
                    .findByProcessInstanceKey(instance.getKey())
                    .spliterator(),
                false)
            .collect(Collectors.toList());

    final ProcessInstanceDto dto = new ProcessInstanceDto();
    dto.setProcessInstanceKey(instance.getKey());

    dto.setPartitionId(instance.getPartitionId());

    dto.setProcessDefinitionKey(instance.getProcessDefinitionKey());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setVersion(instance.getVersion());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(instance.getState());
    dto.setRunning(!isEnded);

    dto.setStartTime(Instant.ofEpochMilli(instance.getStart()).toString());

    if (isEnded) {
      dto.setEndTime(Instant.ofEpochMilli(instance.getEnd()).toString());
    }

    if (instance.getParentElementInstanceKey() > 0) {
      dto.setParentProcessInstanceKey(instance.getParentProcessInstanceKey());

      processInstanceRepository
          .findByKey(instance.getParentProcessInstanceKey())
          .ifPresent(
              parent -> {
                dto.setParentBpmnProcessId(parent.getBpmnProcessId());
              });
    }

    final List<String> completedActivities =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> !e.getBpmnElementType().equals(BpmnElementType.PROCESS.name()))
            .map(ElementInstanceEntity::getElementId)
            .collect(Collectors.toList());

    final List<String> activeActivities =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .filter(e -> !e.getBpmnElementType().equals(BpmnElementType.PROCESS.name()))
            .map(ElementInstanceEntity::getElementId)
            .filter(id -> !completedActivities.contains(id))
            .collect(Collectors.toList());
    dto.setActiveActivities(activeActivities);

    final List<String> takenSequenceFlows =
        events.stream()
            .filter(e -> e.getIntent().equals("SEQUENCE_FLOW_TAKEN"))
            .map(ElementInstanceEntity::getElementId)
            .collect(Collectors.toList());
    dto.setTakenSequenceFlows(takenSequenceFlows);

    final Map<String, Long> completedElementsById =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> !EXCLUDE_ELEMENT_TYPES.contains(e.getBpmnElementType()))
            .collect(
                Collectors.groupingBy(ElementInstanceEntity::getElementId, Collectors.counting()));

    final Map<String, Long> enteredElementsById =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .filter(e -> !EXCLUDE_ELEMENT_TYPES.contains(e.getBpmnElementType()))
            .collect(
                Collectors.groupingBy(ElementInstanceEntity::getElementId, Collectors.counting()));

    final List<ElementInstanceState> elementStates =
        enteredElementsById.entrySet().stream()
            .map(
                e -> {
                  final String elementId = e.getKey();

                  final long enteredInstances = e.getValue();
                  final long completedInstances = completedElementsById.getOrDefault(elementId, 0L);

                  final ElementInstanceState state = new ElementInstanceState();
                  state.setElementId(elementId);
                  state.setActiveInstances(enteredInstances - completedInstances);
                  state.setEndedInstances(completedInstances);

                  return state;
                })
            .collect(Collectors.toList());

    dto.setElementInstances(elementStates);

    final var bpmnModelInstance =
        processRepository
            .findByKey(instance.getProcessDefinitionKey())
            .map(w -> new ByteArrayInputStream(w.getResource().getBytes()))
            .map(Bpmn::readModelFromStream);

    final Map<String, String> flowElements = new HashMap<>();

    bpmnModelInstance.ifPresent(
        bpmn -> {
          bpmn.getModelElementsByType(FlowElement.class)
              .forEach(
                  e -> flowElements.put(e.getId(), Optional.ofNullable(e.getName()).orElse("")));

          dto.setBpmnElementInfos(getBpmnElementInfos(bpmn));
        });

    final List<AuditLogEntry> auditLogEntries =
        events.stream()
            .map(
                e -> {
                  final AuditLogEntry entry = new AuditLogEntry();

                  entry.setKey(e.getKey());
                  entry.setFlowScopeKey(e.getFlowScopeKey());
                  entry.setElementId(e.getElementId());
                  entry.setElementName(flowElements.getOrDefault(e.getElementId(), ""));
                  entry.setBpmnElementType(e.getBpmnElementType());
                  entry.setState(e.getIntent());
                  entry.setTimestamp(Instant.ofEpochMilli(e.getTimestamp()).toString());

                  return entry;
                })
            .collect(Collectors.toList());

    dto.setAuditLogEntries(auditLogEntries);

    final List<IncidentEntity> incidents =
        StreamSupport.stream(
                incidentRepository.findByProcessInstanceKey(instance.getKey()).spliterator(), false)
            .collect(Collectors.toList());

    final Map<Long, String> elementIdsForKeys = new HashMap<>();
    elementIdsForKeys.put(instance.getKey(), instance.getBpmnProcessId());
    events.forEach(e -> elementIdsForKeys.put(e.getKey(), e.getElementId()));

    final List<IncidentDto> incidentDtos =
        incidents.stream()
            .map(
                i -> {
                  final long incidentKey = i.getKey();

                  final IncidentDto incidentDto = new IncidentDto();
                  incidentDto.setKey(incidentKey);

                  incidentDto.setElementId(elementIdsForKeys.get(i.getElementInstanceKey()));
                  incidentDto.setElementInstanceKey(i.getElementInstanceKey());

                  if (i.getJobKey() > 0) {
                    incidentDto.setJobKey(i.getJobKey());
                  }

                  incidentDto.setErrorType(i.getErrorType());
                  incidentDto.setErrorMessage(i.getErrorMessage());

                  final boolean isResolved = i.getResolved() != null && i.getResolved() > 0;
                  incidentDto.setResolved(isResolved);

                  incidentDto.setCreatedTime(Instant.ofEpochMilli(i.getCreated()).toString());

                  if (isResolved) {
                    incidentDto.setResolvedTime(Instant.ofEpochMilli(i.getResolved()).toString());

                    incidentDto.setState("Resolved");
                  } else {
                    incidentDto.setState("Created");
                  }

                  return incidentDto;
                })
            .collect(Collectors.toList());
    dto.setIncidents(incidentDtos);

    final List<String> activitiesWitIncidents =
        incidents.stream()
            .filter(i -> i.getResolved() == null || i.getResolved() <= 0)
            .map(i -> elementIdsForKeys.get(i.getElementInstanceKey()))
            .distinct()
            .collect(Collectors.toList());

    dto.setIncidentActivities(activitiesWitIncidents);

    activeActivities.removeAll(activitiesWitIncidents);
    dto.setActiveActivities(activeActivities);

    final Map<VariableTuple, List<VariableEntity>> variablesByScopeAndName =
        variableRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .collect(Collectors.groupingBy(v -> new VariableTuple(v.getScopeKey(), v.getName())));
    variablesByScopeAndName.forEach(
        (scopeKeyName, variables) -> {
          final VariableEntry variableDto = new VariableEntry();
          final long scopeKey = scopeKeyName.scopeKey;

          variableDto.setScopeKey(scopeKey);
          variableDto.setElementId(elementIdsForKeys.get(scopeKey));

          variableDto.setName(scopeKeyName.name);

          final VariableEntity lastUpdate = variables.get(variables.size() - 1);
          variableDto.setValue(lastUpdate.getValue());
          variableDto.setTimestamp(Instant.ofEpochMilli(lastUpdate.getTimestamp()).toString());

          final List<VariableUpdateEntry> varUpdates =
              variables.stream()
                  .map(
                      v -> {
                        final VariableUpdateEntry varUpdate = new VariableUpdateEntry();
                        varUpdate.setValue(v.getValue());
                        varUpdate.setTimestamp(Instant.ofEpochMilli(v.getTimestamp()).toString());
                        return varUpdate;
                      })
                  .collect(Collectors.toList());
          variableDto.setUpdates(varUpdates);

          dto.getVariables().add(variableDto);
        });

    final List<ActiveScope> activeScopes = new ArrayList<>();
    if (!isEnded) {
      activeScopes.add(new ActiveScope(instance.getKey(), instance.getBpmnProcessId()));

      final List<Long> completedElementInstances =
          events.stream()
              .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .collect(Collectors.toList());

      final List<ActiveScope> activeElementInstances =
          events.stream()
              .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .filter(id -> !completedElementInstances.contains(id))
              .map(scopeKey -> new ActiveScope(scopeKey, elementIdsForKeys.get(scopeKey)))
              .collect(Collectors.toList());

      activeScopes.addAll(activeElementInstances);
    }
    dto.setActiveScopes(activeScopes);

    final List<JobDto> jobDtos =
        jobRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(
                job -> {
                  final JobDto jobDto = toDto(job);
                  jobDto.setElementId(
                      elementIdsForKeys.getOrDefault(job.getElementInstanceKey(), ""));

                  final boolean isActivatable =
                      job.getRetries() > 0
                          && Arrays.asList("created", "failed", "timed_out", "retries_updated")
                              .contains(job.getState());
                  jobDto.setActivatable(isActivatable);

                  return jobDto;
                })
            .collect(Collectors.toList());
    dto.setJobs(jobDtos);

    final List<MessageSubscriptionDto> messageSubscriptions =
        messageSubscriptionRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(
                subscription -> {
                  final MessageSubscriptionDto subscriptionDto = toDto(subscription);
                  subscriptionDto.setElementId(
                      elementIdsForKeys.getOrDefault(subscriptionDto.getElementInstanceKey(), ""));

                  return subscriptionDto;
                })
            .collect(Collectors.toList());
    dto.setMessageSubscriptions(messageSubscriptions);

    final List<TimerDto> timers =
        timerRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    dto.setTimers(timers);

    final var calledProcessInstances =
        processInstanceRepository.findByParentProcessInstanceKey(instance.getKey()).stream()
            .map(
                childEntity -> {
                  final var childDto = new CalledProcessInstanceDto();

                  childDto.setChildProcessInstanceKey(childEntity.getKey());
                  childDto.setChildBpmnProcessId(childEntity.getBpmnProcessId());
                  childDto.setChildState(childEntity.getState());

                  childDto.setElementInstanceKey(childEntity.getParentElementInstanceKey());

                  final var callElementId =
                      elementIdsForKeys.getOrDefault(childEntity.getParentElementInstanceKey(), "");
                  childDto.setElementId(callElementId);

                  return childDto;
                })
            .collect(Collectors.toList());
    dto.setCalledProcessInstances(calledProcessInstances);

    final var errors =
        errorRepository.findByProcessInstanceKey(instance.getKey()).stream()
            .map(this::toDto)
            .collect(Collectors.toList());
    dto.setErrors(errors);

    return dto;
  }

  private List<BpmnElementInfo> getBpmnElementInfos(final BpmnModelInstance bpmn) {
    final List<BpmnElementInfo> infos = new ArrayList<>();

    bpmn.getModelElementsByType(ServiceTask.class)
        .forEach(
            t -> {
              final var info = new BpmnElementInfo();
              info.setElementId(t.getId());
              final var jobType = t.getSingleExtensionElement(ZeebeTaskDefinition.class).getType();
              info.setInfo("job-type: " + jobType);

              infos.add(info);
            });

    bpmn.getModelElementsByType(SequenceFlow.class)
        .forEach(
            s -> {
              final var conditionExpression = s.getConditionExpression();

              if (conditionExpression != null && !conditionExpression.getTextContent().isEmpty()) {

                final var info = new BpmnElementInfo();
                info.setElementId(s.getId());
                final var condition = conditionExpression.getTextContent();
                info.setInfo("condition: " + condition);

                infos.add(info);
              }
            });

    bpmn.getModelElementsByType(CatchEvent.class)
        .forEach(
            catchEvent -> {
              final var info = new BpmnElementInfo();
              info.setElementId(catchEvent.getId());

              catchEvent
                  .getEventDefinitions()
                  .forEach(
                      eventDefinition -> {
                        if (eventDefinition instanceof ErrorEventDefinition) {
                          final var errorEventDefinition = (ErrorEventDefinition) eventDefinition;
                          final var errorCode = errorEventDefinition.getError().getErrorCode();

                          info.setInfo("errorCode: " + errorCode);
                          infos.add(info);
                        }

                        if (eventDefinition instanceof TimerEventDefinition) {
                          final var timerEventDefinition = (TimerEventDefinition) eventDefinition;

                          Optional.<ModelElementInstance>ofNullable(
                                  timerEventDefinition.getTimeCycle())
                              .or(() -> Optional.ofNullable(timerEventDefinition.getTimeDate()))
                              .or(() -> Optional.ofNullable(timerEventDefinition.getTimeDuration()))
                              .map(ModelElementInstance::getTextContent)
                              .ifPresent(
                                  timer -> {
                                    info.setInfo("timer: " + timer);
                                    infos.add(info);
                                  });
                        }
                      });
            });

    return infos;
  }

  @GetMapping("/views/incidents")
  @Transactional
  public String incidentList(final Map<String, Object> model, final Pageable pageable) {

    final long count = incidentRepository.countByResolvedIsNull();

    final List<IncidentListDto> incidents = new ArrayList<>();
    for (final IncidentEntity incidentEntity : incidentRepository.findByResolvedIsNull(pageable)) {
      final IncidentListDto dto = toDto(incidentEntity);
      incidents.add(dto);
    }

    model.put("incidents", incidents);
    model.put("count", count);

    addCommonVariablesToModel(model);
    addPaginationToModel(model, pageable, count);

    return "incident-list-view";
  }

  private IncidentListDto toDto(final IncidentEntity incident) {
    final IncidentListDto dto = new IncidentListDto();
    dto.setKey(incident.getKey());

    dto.setBpmnProcessId(incident.getBpmnProcessId());
    dto.setProcessDefinitionKey(incident.getProcessDefinitionKey());
    dto.setProcessInstanceKey(incident.getProcessInstanceKey());

    dto.setErrorType(incident.getErrorType());
    dto.setErrorMessage(incident.getErrorMessage());

    final boolean isResolved = incident.getResolved() != null && incident.getResolved() > 0;

    dto.setCreatedTime(Instant.ofEpochMilli(incident.getCreated()).toString());

    if (isResolved) {
      dto.setResolvedTime(Instant.ofEpochMilli(incident.getResolved()).toString());

      dto.setState("Resolved");
    } else {
      dto.setState("Created");
    }

    return dto;
  }

  @GetMapping("/views/jobs")
  public String jobList(final Map<String, Object> model, final Pageable pageable) {

    final long count = jobRepository.countByStateNotIn(JOB_COMPLETED_INTENTS);

    final List<JobDto> dtos = new ArrayList<>();
    for (final JobEntity jobEntity :
        jobRepository.findByStateNotIn(JOB_COMPLETED_INTENTS, pageable)) {
      final JobDto dto = toDto(jobEntity);
      dtos.add(dto);
    }

    model.put("jobs", dtos);
    model.put("count", count);

    addCommonVariablesToModel(model);
    addPaginationToModel(model, pageable, count);

    return "job-list-view";
  }

  private JobDto toDto(final JobEntity job) {
    final JobDto dto = new JobDto();

    dto.setKey(job.getKey());
    dto.setJobType(job.getJobType());
    dto.setProcessInstanceKey(job.getProcessInstanceKey());
    dto.setElementInstanceKey(job.getElementInstanceKey());
    dto.setState(job.getState());
    dto.setRetries(job.getRetries());
    Optional.ofNullable(job.getWorker()).ifPresent(dto::setWorker);
    dto.setTimestamp(Instant.ofEpochMilli(job.getTimestamp()).toString());

    return dto;
  }

  @GetMapping("/views/messages")
  public String messageList(final Map<String, Object> model, final Pageable pageable) {

    final long count = messageRepository.count();

    final List<MessageDto> dtos = new ArrayList<>();
    for (final MessageEntity messageEntity : messageRepository.findAll(pageable)) {
      final MessageDto dto = toDto(messageEntity);
      dtos.add(dto);
    }

    model.put("messages", dtos);
    model.put("count", count);

    addCommonVariablesToModel(model);
    addPaginationToModel(model, pageable, count);

    return "message-list-view";
  }

  @GetMapping("/views/errors")
  public String errorList(final Map<String, Object> model, final Pageable pageable) {

    final long count = errorRepository.count();

    final List<ErrorDto> dtos = new ArrayList<>();
    for (final ErrorEntity entity : errorRepository.findAll(pageable)) {
      final var dto = toDto(entity);
      dtos.add(dto);
    }

    model.put("errors", dtos);
    model.put("count", count);

    addCommonVariablesToModel(model);
    addPaginationToModel(model, pageable, count);

    return "error-list-view";
  }

  private MessageDto toDto(final MessageEntity message) {
    final MessageDto dto = new MessageDto();

    dto.setKey(message.getKey());
    dto.setName(message.getName());
    dto.setCorrelationKey(message.getCorrelationKey());
    dto.setMessageId(message.getMessageId());
    dto.setPayload(message.getPayload());
    dto.setState(message.getState());
    dto.setTimestamp(Instant.ofEpochMilli(message.getTimestamp()).toString());

    return dto;
  }

  private MessageSubscriptionDto toDto(final MessageSubscriptionEntity subscription) {
    final MessageSubscriptionDto dto = new MessageSubscriptionDto();

    dto.setKey(subscription.getId());
    dto.setMessageName(subscription.getMessageName());
    dto.setCorrelationKey(Optional.ofNullable(subscription.getCorrelationKey()).orElse(""));

    dto.setProcessInstanceKey(subscription.getProcessInstanceKey());
    dto.setElementInstanceKey(subscription.getElementInstanceKey());

    dto.setElementId(subscription.getTargetFlowNodeId());

    dto.setState(subscription.getState());
    dto.setTimestamp(Instant.ofEpochMilli(subscription.getTimestamp()).toString());

    dto.setOpen(subscription.getState().equalsIgnoreCase(MessageSubscriptionIntent.CREATED.name()));

    return dto;
  }

  private TimerDto toDto(final TimerEntity timer) {
    final TimerDto dto = new TimerDto();

    dto.setElementId(timer.getTargetElementId());
    dto.setState(timer.getState());
    dto.setDueDate(Instant.ofEpochMilli(timer.getDueDate()).toString());
    dto.setTimestamp(Instant.ofEpochMilli(timer.getTimestamp()).toString());
    dto.setElementInstanceKey(timer.getElementInstanceKey());

    final int repetitions = timer.getRepetitions();
    dto.setRepetitions(repetitions >= 0 ? String.valueOf(repetitions) : "∞");

    return dto;
  }

  private ErrorDto toDto(final ErrorEntity entity) {
    final var dto = new ErrorDto();
    dto.setPosition(entity.getPosition());
    dto.setErrorEventPosition(entity.getErrorEventPosition());
    dto.setExceptionMessage(entity.getExceptionMessage());
    dto.setStacktrace(entity.getStacktrace());
    dto.setTimestamp(Instant.ofEpochMilli(entity.getTimestamp()).toString());

    if (entity.getProcessInstanceKey() > 0) {
      dto.setProcessInstanceKey(entity.getProcessInstanceKey());
    }

    return dto;
  }

  private String getProcessResource(final ProcessEntity process) {
    final var resource = process.getResource();
    // replace all backticks because they are used to enclose the content of the BPMN in the HTML
    return resource.replaceAll("`", "\"");
  }

  private void addCommonVariablesToModel(final Map<String, Object> model) {
    addContextPathToModel(model);
    addWhitelabelingOptionsToModel(model);
  }

  private void addContextPathToModel(final Map<String, Object> model) {
    model.put("context-path", basePath);
  }

  private void addWhitelabelingOptionsToModel(final Map<String, Object> model) {
    model.put("logo-path", logoPath);
    model.put("custom-css-path", customCssPath);
    model.put("custom-js-path", customJsPath);
    model.put("custom-title", customTitle);
  }

  private void addPaginationToModel(
      final Map<String, Object> model, final Pageable pageable, final long count) {

    final int currentPage = pageable.getPageNumber();
    final int prevPage = currentPage - 1;
    final int nextPage = currentPage + 1;
    final int lastPage = getLastPage(pageable, count);

    final var prevPages =
        IntStream.range(currentPage - PAGE_RANGE, currentPage)
            .filter(p -> p > FIRST_PAGE)
            .boxed()
            .map(Page::new)
            .collect(Collectors.toList());
    final var nextPages =
        IntStream.rangeClosed(currentPage + 1, currentPage + PAGE_RANGE)
            .filter(p -> p < lastPage)
            .boxed()
            .map(Page::new)
            .collect(Collectors.toList());
    final var hasPrevGap =
        !prevPages.isEmpty() && prevPages.stream().allMatch(p -> p.pageNumber > FIRST_PAGE + 1);
    final var hasNextGap =
        !nextPages.isEmpty() && nextPages.stream().allMatch(p -> p.pageNumber < lastPage - 1);

    model.put("page", new Page(currentPage));
    model.put("prevPages", prevPages);
    model.put("nextPages", nextPages);
    model.put("hasPrevPagesGap", hasPrevGap);
    model.put("hasNextPagesGap", hasNextGap);

    if (currentPage > 0) {
      model.put("prevPage", new Page(prevPage));
      model.put("firstPage", new Page(FIRST_PAGE));
    }
    if (lastPage > currentPage) {
      model.put("nextPage", new Page(nextPage));
      model.put("lastPage", new Page(lastPage));
    }
  }

  private int getLastPage(final Pageable pageable, final long count) {
    int lastPage = 0;
    if (pageable.getPageSize() > 0) {
      lastPage = (int) count / pageable.getPageSize();
      if (count % pageable.getPageSize() == 0) {
        lastPage--;
      }
    }
    return lastPage;
  }

  private static class Page {
    private final int pageNumber;
    private final int displayNumber;

    private Page(final int pageNumber) {
      this.pageNumber = pageNumber;
      this.displayNumber = pageNumber + 1;
    }

    public int getPageNumber() {
      return pageNumber;
    }

    public int getDisplayNumber() {
      return displayNumber;
    }
  }

  private static class VariableTuple {
    private final long scopeKey;
    private final String name;

    VariableTuple(final long scopeKey, final String name) {
      this.scopeKey = scopeKey;
      this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final VariableTuple that = (VariableTuple) o;
      return scopeKey == that.scopeKey && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scopeKey, name);
    }
  }
}
