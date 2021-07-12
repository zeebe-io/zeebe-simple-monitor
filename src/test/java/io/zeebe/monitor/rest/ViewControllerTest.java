package io.zeebe.monitor.rest;

import io.zeebe.monitor.ZeebeSimpleMonitorApp;
import io.zeebe.monitor.repository.*;
import io.zeebe.monitor.zeebe.ZeebeHazelcastService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(classes = ZeebeSimpleMonitorApp.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "white-label.logo.path: img/test-logo.png",
        "white-label.custom.title: Test Zeebe Simple Monitor",
        "white-label.custom.css.path: css/test-custom.css",
        "white-label.custom.js.path: js/test-custom.js",
    })
public class ViewControllerTest {

  @LocalServerPort
  private int port;

  @Autowired
  private ViewController controller;

  @Autowired
  private TestRestTemplate restTemplate;

  @MockBean
  private HazelcastConfigRepository hazelcastConfigRepository;
  @MockBean
  private ZeebeHazelcastService zeebeHazelcastService;
  @MockBean
  private ProcessRepository processRepository;
  @MockBean
  private ProcessInstanceRepository processInstanceRepository;
  @MockBean
  private ElementInstanceRepository activityInstanceRepository;
  @MockBean
  private IncidentRepository incidentRepository;
  @MockBean
  private JobRepository jobRepository;
  @MockBean
  private MessageRepository messageRepository;
  @MockBean
  private MessageSubscriptionRepository messageSubscriptionRepository;
  @MockBean
  private TimerRepository timerRepository;
  @MockBean
  private VariableRepository variableRepository;
  @MockBean
  private ErrorRepository errorRepository;

  @Before
  public void setUp() throws Exception {
    when(processRepository.findAll(any(Pageable.class))).thenReturn(Page.empty());
  }

  @Test
  public void index_page_successfully_responded() {
    ResponseEntity<String> entity = restTemplate.getForEntity("/", String.class);

    assertThat(entity.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  public void index_page_contains_whitelabeling_title() {
    ResponseEntity<String> entity = restTemplate.getForEntity("/", String.class);

    assertThat(entity.getBody()).contains("Test Zeebe Simple Monitor");
  }

  @Test
  public void index_page_contains_whitelabeling_logo() {
    ResponseEntity<String> entity = restTemplate.getForEntity("/", String.class);

    assertThat(entity.getBody()).contains("<img src=\"/img/test-logo.png\"");
  }

  @Test
  public void index_page_contains_whitelabeling_js() {
    ResponseEntity<String> entity = restTemplate.getForEntity("/", String.class);

    assertThat(entity.getBody()).contains("<script src=\"/js/test-custom.js\"></script>");
  }

  @Test
  public void index_page_contains_whitelabeling_css() {
    ResponseEntity<String> entity = restTemplate.getForEntity("/", String.class);

    assertThat(entity.getBody()).contains("<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/test-custom.css\"/>");
  }

}
