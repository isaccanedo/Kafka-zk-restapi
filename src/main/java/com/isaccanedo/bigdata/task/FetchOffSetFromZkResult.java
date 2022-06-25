package com.isaccanedo.bigdata.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FetchOffSetFromZkResult {

  private String topic;
  private int parition;
  private long offset;
}
