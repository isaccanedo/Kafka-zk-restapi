package com.isaccanedo.bigdata.validator;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import com.isaccanedo.bigdata.service.KafkaAdminService;
import org.springframework.beans.factory.annotation.Autowired;

public class TopicExistValidator implements ConstraintValidator<TopicExistConstraint, String> {

  @Autowired private KafkaAdminService kafkaAdminService;

  public void initialize(TopicExistConstraint constraint) {}

  public boolean isValid(String topic, ConstraintValidatorContext context) {
    return kafkaAdminService.existTopic(topic);
  }
}
