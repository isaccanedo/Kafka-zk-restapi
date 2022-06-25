package com.isaccanedo.bigdata.model;

import java.util.List;

import lombok.*;
import lombok.extern.log4j.Log4j2;


@Getter
@Setter
@Log4j2
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddPartition {

  String topic;
  int numPartitionsAdded;
  List<List<Integer>> replicaAssignment;
}
