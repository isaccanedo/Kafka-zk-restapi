package com.isaccanedo.bigdata.model;

import java.util.List;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.Node;
import com.isaccanedo.bigdata.constant.ConsumerGroupState;

@Data
@Builder
public class ConsumerGroupMeta {

  private String groupId;
  private ConsumerGroupState state;
  private String assignmentStrategy;
  private Node coordinator;
  private List<MemberDescription> members;
}
