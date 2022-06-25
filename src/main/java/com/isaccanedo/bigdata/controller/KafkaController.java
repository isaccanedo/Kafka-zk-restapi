package com.isaccanedo.bigdata.controller;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import joptsimple.internal.Strings;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import com.isaccanedo.bigdata.constant.ConsumerType;
import com.isaccanedo.bigdata.model.AddPartition;
import com.isaccanedo.bigdata.model.BrokerInfo;
import com.isaccanedo.bigdata.model.ClusterInfo;
import com.isaccanedo.bigdata.model.ConsumerGroupDesc;
import com.isaccanedo.bigdata.model.ConsumerGroupMeta;
import com.isaccanedo.bigdata.model.CustomConfigEntry;
import com.isaccanedo.bigdata.model.GeneralResponse;
import com.isaccanedo.bigdata.model.HealthCheckResult;
import com.isaccanedo.bigdata.model.ReassignModel;
import com.isaccanedo.bigdata.model.ReassignStatus;
import com.isaccanedo.bigdata.model.ReassignWrapper;
import com.isaccanedo.bigdata.model.Record;
import com.isaccanedo.bigdata.model.TopicBrief;
import com.isaccanedo.bigdata.model.TopicDetail;
import com.isaccanedo.bigdata.model.TopicMeta;
import com.isaccanedo.bigdata.service.KafkaAdminService;
import com.isaccanedo.bigdata.validator.ConsumerGroupExistConstraint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by gnuhpc on 2017/7/16.
 */
@Log4j2
@RequestMapping("/kafka")
@RestController
public class KafkaController {

  @Lazy
  @Autowired
  private KafkaAdminService kafkaAdminService;

//  @Autowired private KafkaProducerService kafkaProducerService;

  @GetMapping(value = "/cluster")
  @ApiOperation(value = "Describe cluster, nodes, controller info.")
  public ClusterInfo describeCluster() {
    return kafkaAdminService.describeCluster();
  }

  @GetMapping(value = "/brokers")
  @ApiOperation(value = "List brokers in this cluster")
  public List<BrokerInfo> listBrokers() {
    return kafkaAdminService.listBrokers();
  }

  @GetMapping(value = "/controller")
  @ApiOperation(value = "Get controller in this cluster")
  public Node getControllerId() {
    return kafkaAdminService.getController();
  }

  @GetMapping(value = "/brokers/logdirs")
  @ApiOperation(value = "List log dirs by broker list")
  public Map<Integer, List<String>> listLogDirs(
      @RequestParam(required = false) List<Integer> brokerList) {
    return kafkaAdminService.listLogDirsByBroker(brokerList);
  }

  @PostMapping(value = "/brokers/logdirs/detail")
  @ApiOperation(value = "Describe log dirs by broker list and topic list")
  public Map<Integer, Map<String, LogDirInfo>> describeLogDirs(
      @RequestParam(required = false) List<Integer> brokerList,
      @RequestParam(required = false) List<String> logDirList,
      @RequestBody(required = false) Map<String, List<Integer>> topicPartitionMap) {
    return kafkaAdminService
        .describeLogDirsByBrokerAndTopic(brokerList, logDirList, topicPartitionMap);
  }

  @GetMapping(value = "/brokers/replicalogdir/{brokerId}/{topic}/{partition}")
  @ApiOperation(value = "Describe replica log dir.")
  public ReplicaLogDirInfo describeReplicaLogDirs(@PathVariable int brokerId,
      @PathVariable String topic, @PathVariable int partition) {
    TopicPartitionReplica replica = new TopicPartitionReplica(topic, partition, brokerId);
    return kafkaAdminService.describeReplicaLogDir(replica);
  }

  @GetMapping(value = "/brokers/{brokerId}/conf")
  @ApiOperation(value = "Get broker configs, including dynamic configs")
  public Collection<CustomConfigEntry> getBrokerConfig(@PathVariable int brokerId) {
    return kafkaAdminService.getBrokerConf(brokerId);
  }

  @GetMapping(value = "/brokers/{brokerId}/dynconf")
  @ApiOperation(value = "Get broker dynamic configs")
  public Properties getBrokerDynConfig(@PathVariable int brokerId) {
    return kafkaAdminService.getConfigInZk(Type.BROKER, String.valueOf(brokerId));
  }

  @PutMapping(value = "/brokers/{brokerId}/dynconf")
  @ApiOperation(value = "Update broker configs")
  public Properties updateBrokerDynConfig(
      @PathVariable int brokerId, @RequestBody Properties props) {
    return kafkaAdminService.updateBrokerDynConf(brokerId, props);
  }

  @DeleteMapping(value = "/brokers/{brokerId}/dynconf")
  @ApiOperation(value = "Remove broker dynamic configs")
  public void removeBrokerDynConfig(
      @PathVariable int brokerId, @RequestParam List<String> configKeysToBeRemoved) {
    kafkaAdminService.removeConfigInZk(
        Type.BROKER, String.valueOf(brokerId), configKeysToBeRemoved);
  }

  @GetMapping("/topics")
  @ApiOperation(value = "List topics")
  public List<String> listTopics() {
    return kafkaAdminService.listTopics();
  }

  @GetMapping("/topicsbrief")
  @ApiOperation(value = "List topics Brief")
  public List<TopicBrief> listTopicBrief() {
    return kafkaAdminService.listTopicBrief();
  }

  @PostMapping(value = "/topics/create", consumes = "application/json")
  @ResponseStatus(HttpStatus.CREATED)
  @ApiOperation(value = "Create topics")
  @ApiParam(value = "if reassignStr set, partitions and repli-factor will be ignored.")
  public HashMap<String, GeneralResponse> createTopic(
      @RequestBody List<TopicDetail> topicList) {
    return kafkaAdminService.createTopic(topicList);
  }

  @PostMapping(value = "/topics/create/check", consumes = "application/json")
  @ApiOperation(value = "Create topics check")
  public Map createTopicCheck(
      @RequestBody List<TopicDetail> topicList) {
    return kafkaAdminService.createTopicCheck(topicList);
  }

  @ApiOperation(value = "Tell if a topic exists")
  @GetMapping(value = "/topics/{topic}/exist")
  public boolean existTopic(@PathVariable String topic) {
    return kafkaAdminService.existTopic(topic);
  }

//  @PostMapping(value = "/topics/{topic}/write", consumes = "text/plain")
//  @ResponseStatus(HttpStatus.CREATED)
//  @ApiOperation(value = "Write a message to the topic, for testing purpose")
//  public GeneralResponse writeMessage(@PathVariable String topic, @RequestBody String message) {
//    kafkaProducerService.send(topic, message);
//    return GeneralResponse.builder()
//        .state(GeneralResponseState.success)
//        .msg(message + " has been sent")
//        .build();
//  }

  @GetMapping(value = "/consumer/{topic}/{partition}/{offset}")
  @ApiOperation(
      value =
          "Get the message from the offset of the partition in the topic")
  public List<Record> getMessage(
      @PathVariable String topic,
      @PathVariable int partition,
      @PathVariable
      @ApiParam(
          value =
              "[long/yyyy-MM-dd HH:mm:ss.SSS] can be supported. ")
          String offset,
      @RequestParam(required = false, defaultValue = "10") int maxRecords,
      @RequestParam(required = false, defaultValue = "StringDeserializer") String keyDecoder,
      @RequestParam(required = false, defaultValue = "StringDeserializer") String valueDecoder,
      @RequestParam(required = false) String avroSchema,
      @RequestParam(required = false, defaultValue = "30000") long fetchTimeoutMs)
      throws ApiException {
    long offsetL;
    if (kafkaAdminService.isDateTime(offset)) {
      offsetL = kafkaAdminService.getOffsetByTimestamp(topic, partition, offset);
    } else {
      offsetL = Long.parseLong(offset);
    }
    if (offsetL >= 0) {
      return kafkaAdminService.getRecordsByOffset(topic, partition, offsetL, maxRecords, keyDecoder,
          valueDecoder, avroSchema, fetchTimeoutMs);
    } else {
      return new ArrayList<>();
    }
  }

  @GetMapping(value = "/topics/{topic}")
  @ApiOperation(value = "Describe a topic by fetching the metadata and config")
  public TopicMeta describeTopic(@PathVariable String topic) {
    return kafkaAdminService.describeTopic(topic);
  }

  @DeleteMapping(value = "/topics")
  @ApiOperation(value = "Delete a topic list (you should enable topic deletion")
  public Map<String, GeneralResponse> deleteTopicList(@RequestParam List<String> topicList) {
    // TODO add a function to delete topics completely,
    // rmr /brokers/topics/< topic_name >
    // rmr /config/topics/< topic_name >
    // rmr /admin/delete_topics/< topic_name >
    // rm log dirs on all brokers
    return kafkaAdminService.deleteTopicList(topicList);
  }

  @PutMapping(value = "/topics/{topic}/conf")
  @ApiOperation(value = "Update topic configs")
  public Collection<CustomConfigEntry> updateTopicConfig(
      @PathVariable String topic, @RequestBody Properties props) {
    return kafkaAdminService.updateTopicConf(topic, props);
  }

  @GetMapping(value = "/topics/{topic}/conf")
  @ApiOperation(value = "Get topic configs")
  public Collection<CustomConfigEntry> getTopicConfig(@PathVariable String topic) {
    return kafkaAdminService.getTopicConf(topic);
  }

  @GetMapping(value = "/topics/{topic}/dynconf")
  @ApiOperation(value = "Get topic dyn configs")
  public Properties getTopicDynConfig(@PathVariable String topic) {
    return kafkaAdminService.getConfigInZk(Type.TOPIC, topic);
  }

  @GetMapping(value = "/topics/{topic}/conf/{key}")
  @ApiOperation(value = "Get topic config by key")
  public Properties getTopicConfigByKey(@PathVariable String topic, @PathVariable String key) {
    return kafkaAdminService.getTopicConfByKey(topic, key);
  }

  @PutMapping(value = "/topics/{topic}/conf/{key}={value}")
  @ApiOperation(value = "Update a topic config by key")
  public Collection<CustomConfigEntry> updateTopicConfigByKey(
      @PathVariable String topic, @PathVariable String key, @PathVariable String value) {
    return kafkaAdminService.updateTopicConfByKey(topic, key, value);
  }

  @PostMapping(value = "/partitions/add")
  @ApiOperation(value = "Add partitions to the topics")
  public Map<String, GeneralResponse> addPartition(@RequestBody List<AddPartition> addPartitions) {
    return kafkaAdminService.addPartitions(addPartitions);
  }

  @PostMapping(value = "/partitions/reassign/generate")
  @ApiOperation(value = "Generate plan for the partition reassignment")
  public List<ReassignModel> generateReassignPartitions(
      @RequestBody ReassignWrapper reassignWrapper) {
    return kafkaAdminService.generateReassignPartition(reassignWrapper);
  }

  @PutMapping(value = "/partitions/reassign/execute")
  @ApiOperation(value = "Execute the partition reassignment")
  public ReassignStatus executeReassignPartitions(
      @RequestBody ReassignModel reassign,
      @RequestParam(required = false, defaultValue = "-1") long interBrokerThrottle,
      @RequestParam(required = false, defaultValue = "-1") long replicaAlterLogDirsThrottle,
      @RequestParam(required = false, defaultValue = "10000") long timeoutMs) {
    return kafkaAdminService.executeReassignPartition(
        reassign, interBrokerThrottle, replicaAlterLogDirsThrottle, timeoutMs);
  }

  @PutMapping(value = "/partitions/reassign/check")
  @ApiOperation(value = "Check the partition reassignment process")
  @ApiResponses(
      value = {
          @ApiResponse(code = 1, message = "Reassignment Completed"),
          @ApiResponse(code = 0, message = "Reassignment In Progress"),
          @ApiResponse(code = -1, message = "Reassignment Failed")
      })
  public ReassignStatus checkReassignPartitions(@RequestBody ReassignModel reassign) {
    return kafkaAdminService.checkReassignStatus(reassign);
  }

  @PutMapping(value = "/partitions/preferredreplica/elect")
  @ApiOperation(value = "Move partition leader to preferred replica.")
  @ApiResponses(
      value = {
          @ApiResponse(code = -2, message = "Partition doesn't exist"),
          @ApiResponse(code = -1, message = "Other preferred replica elect is in progress"),
          @ApiResponse(code = 0, message = "Successfully started preferred replica election"),
      })
  public Map<com.isaccanedo.bigdata.model.TopicPartition, Integer> preferredReplicaElection(
      @RequestBody List<com.isaccanedo.bigdata.model.TopicPartition> partitionList) {
    return kafkaAdminService.moveLeaderToPreferredReplica(partitionList);
  }

  @PutMapping(value = "/partitions/reassign/stop")
  @ApiOperation(value = "Stop the partition reassignment process")
  public GeneralResponse stopReassignPartitions() {
    return kafkaAdminService.stopReassignPartitions();
  }

  @GetMapping(value = "/consumergroups")
  @ApiOperation(value = "List all consumer groups from zk and kafka")
  public Map<String, Set<String>> listAllConsumerGroups(
      @RequestParam(required = false) ConsumerType type,
      @RequestParam(required = false) String topic) {
    if (topic != null) {
      return kafkaAdminService.listConsumerGroupsByTopic(topic, type);
    } else {
      return kafkaAdminService.listAllConsumerGroups(type);
    }
  }

  @GetMapping(value = "/consumergroups/{consumerGroup}/{type}/topic")
  @ApiOperation(value = "Get the topics involved of the specify consumer group")
  public Set<String> listTopicByConsumerGroup(
      @PathVariable String consumerGroup, @PathVariable ConsumerType type) {
    return kafkaAdminService.listTopicsByConsumerGroup(consumerGroup, type);
  }

  @GetMapping(value = "/consumergroups/{consumerGroup}/meta")
  @ApiOperation(
      value =
          "Get the meta data of the specify new consumer group, including state, coordinator,"
              + " assignmentStrategy, members")
  public ConsumerGroupMeta getConsumerGroupMeta(@PathVariable String consumerGroup) {
    if (kafkaAdminService.isNewConsumerGroup(consumerGroup)) {
      return kafkaAdminService.getConsumerGroupMeta(consumerGroup);
    }

    throw new ApiException("New consumer group:" + consumerGroup + " non-exist.");
  }

  @GetMapping(value = "/consumergroups/meta")
  @ApiOperation(
      value =
          "Get all the meta data of new consumer groups, including state, coordinator,"
              + " assignmentStrategy, members")
  public List<ConsumerGroupMeta> getConsumerGroupsMeta() {
    Set<String> consumerGroupList = kafkaAdminService.listAllNewConsumerGroups();
    List<ConsumerGroupMeta> consumerGroupMetaList = new ArrayList<>();
    for (String consumerGroup : consumerGroupList) {
      if (kafkaAdminService.isNewConsumerGroup(consumerGroup)) {
        consumerGroupMetaList.add(kafkaAdminService.getConsumerGroupMeta(consumerGroup));
      } else {
        throw new ApiException("New consumer group:" + consumerGroup + " non-exist.");
      }
    }

    return consumerGroupMetaList;
  }

  @GetMapping(value = "/consumergroups/{type}/topic/{topic}")
  @ApiOperation(value = "Describe consumer groups by topic, showing lag and offset")
  public List<ConsumerGroupDesc> describeConsumerGroupByTopic(
      @RequestParam(required = false) String consumerGroup,
      @PathVariable ConsumerType type,
      @PathVariable String topic) {
    if (!Strings.isNullOrEmpty(topic)) {
      existTopic(topic);
    } else {
      throw new ApiException("Topic must be set!");
    }
    if (type != null && type == ConsumerType.NEW) {
      return kafkaAdminService.describeNewConsumerGroupByTopic(consumerGroup, topic);
    }

    if (type != null && type == ConsumerType.OLD) {
      return kafkaAdminService.describeOldConsumerGroupByTopic(consumerGroup, topic);
    }

    throw new ApiException("Unknown type specified!");
  }

  @GetMapping(value = "/consumergroups/{consumerGroup}/{type}")
  @ApiOperation(
      value =
          "Describe consumer group, showing lag and offset, may be slow if multi"
              + " topics are listened")
  public Map<String, List<ConsumerGroupDesc>> describeConsumerGroup(
      @ConsumerGroupExistConstraint @PathVariable String consumerGroup,
      @PathVariable ConsumerType type) {
    return kafkaAdminService.describeConsumerGroup(consumerGroup, type);
  }

  @PutMapping(value = "/consumergroup/{consumergroup}/{type}/topic/{topic}/{partition}/{offset}")
  @ApiOperation(
      value =
          "Reset consumer group offset, earliest/latest can be used. Support reset by time for "
              + "new consumer group, pass a parameter that satisfies yyyy-MM-dd HH:mm:ss.SSS "
              + "to offset.")
  public GeneralResponse resetOffset(
      @PathVariable String topic,
      @PathVariable int partition,
      @PathVariable String consumergroup,
      @PathVariable
      @ApiParam(
          value =
              "[earliest/latest/{long}/yyyy-MM-dd HH:mm:ss.SSS] can be supported. "
                  + "The date type is only valid for new consumer group.")
          String offset,
      @PathVariable ConsumerType type) {
    return kafkaAdminService.resetOffset(topic, partition, consumergroup, type, offset);
  }

  @GetMapping(value = "/consumergroup/{consumergroup}/{type}/topic/{topic}/lastcommittime")
  public Map<String, Map<Integer, Long>> getLastCommitTimestamp(
      @PathVariable String consumergroup,
      @PathVariable String topic,
      @PathVariable ConsumerType type) {
    return kafkaAdminService.getLastCommitTime(consumergroup, topic, type);
  }

  @DeleteMapping(value = "/consumergroup/{consumergroup}/{type}")
  @ApiOperation(value = "Delete Consumer Group")
  public GeneralResponse deleteOldConsumerGroup(
      @PathVariable String consumergroup, @PathVariable ConsumerType type) {
    return kafkaAdminService.deleteConsumerGroup(consumergroup, type);
  }

  @GetMapping(value = "/health")
  @ApiOperation(value = "Check the cluster health.")
  public HealthCheckResult healthCheck() {
    return kafkaAdminService.healthCheck();
  }

  //TODO add kafkaAdminClient.deleterecords api
}
