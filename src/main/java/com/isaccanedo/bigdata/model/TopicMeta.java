package com.isaccanedo.bigdata.model;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Getter
@Setter
public class TopicMeta {

  private String topicName;
  private boolean internal;
  private int partitionCount;
  private int replicationFactor;
  private List<CustomTopicPartitionInfo> topicPartitionInfos;

  public TopicMeta(String topicName) {
    this.topicName = topicName;
  }
}
