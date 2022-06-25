package com.isaccanedo.bigdata.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.HashMap;
import java.util.List;
import javax.validation.constraints.Pattern;
import lombok.extern.log4j.Log4j2;
import com.isaccanedo.bigdata.model.JMXMetricData;
import com.isaccanedo.bigdata.model.JMXMetricDataV1;
import com.isaccanedo.bigdata.model.JMXQuery;
import com.isaccanedo.bigdata.service.CollectorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Log4j2
@RestController
@Validated
@Api(value = "/jmx", description = "Rest API for Collecting JMX Metric Data")
public class CollectorController {

  private static final String IP_AND_PORT_LIST_REGEX =
      "(([0-9]+(?:\\.[0-9]+){3}:[0-9]+,)*([0-9]+(?:\\.[0-9]+){3}:[0-9]+)+)|(default)";
  @Autowired private CollectorService collectorService;

  @Value("${jmx.kafka.jmxurl}")
  private String jmxKafkaURL;

  @GetMapping("/jmx/v1")
  @ApiOperation(value = "Fetch all JMX metric data")
  public List<JMXMetricDataV1> collectJMXMetric(
      @Pattern(regexp = IP_AND_PORT_LIST_REGEX)
          @RequestParam
          @ApiParam(value = "Parameter jmxurl should be a comma-separated list of {IP:Port} or set"
              + " to \'default\'")
          String jmxurl) {
    if (jmxurl.equals("default")) {
      jmxurl = jmxKafkaURL;
    }

    log.debug("Collect JMX Metric Data Started.");
    return collectorService.collectJMXData(jmxurl);
  }

  @PostMapping("/jmx/v2")
  @ApiOperation(value = "Fetch JMX metric data with query filter. You can get the query filter "
      + "template through the API /jmx/v2/filters.")
  public List<JMXMetricData> collectJMXMetric(
      @Pattern(regexp = IP_AND_PORT_LIST_REGEX)
          @RequestParam @ApiParam(value = "Parameter jmxurl should be a comma-separated list of "
          + "{IP:Port} or set to \'default\'")
          String jmxurl,
      @RequestBody JMXQuery jmxQuery) {
    if (jmxurl.equals("default")) {
      jmxurl = jmxKafkaURL;
    }

    log.debug("Collect JMX Metric Data Started.");

    return collectorService.collectJMXData(jmxurl, jmxQuery);
  }

  @GetMapping("/jmx/v2/filters")
  @ApiOperation(value = "List the query filter templates with the filterKey. If filterKey is set "
      + "to empty, it will return all the templates.")
  public HashMap<String, Object> listJMXFilterTemplate(@RequestParam String filterKey) {
    return collectorService.listJMXFilterTemplate(filterKey);
  }
}
