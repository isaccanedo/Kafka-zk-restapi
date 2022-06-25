package com.isaccanedo.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import com.isaccanedo.bigdata.constant.GeneralResponseState;

@Data
@Log4j2
@AllArgsConstructor
@Builder
@ToString
public class GeneralResponse {

  private GeneralResponseState state;
  private String msg;
  private Object data;
}
