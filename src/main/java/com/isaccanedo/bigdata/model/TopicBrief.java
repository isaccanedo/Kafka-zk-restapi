package com.isaccanedo.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicBrief {

  private String topic;
  private int numPartition;
  private double isrRate;
  private int replicationFactor;
}
