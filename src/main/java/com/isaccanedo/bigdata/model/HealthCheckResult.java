package com.isaccanedo.bigdata.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Data
public class HealthCheckResult {

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
  private LocalDateTime timestamp;

  public String status;
  public String msg;

  public HealthCheckResult() {
    this.timestamp = LocalDateTime.now();
  }
}
