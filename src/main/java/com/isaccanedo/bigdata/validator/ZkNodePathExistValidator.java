package com.isaccanedo.bigdata.validator;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import com.isaccanedo.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

public class ZkNodePathExistValidator
    implements ConstraintValidator<ZkNodePathExistConstraint, String> {

  @Lazy
  @Autowired private ZookeeperUtils zookeeperUtils;

  public void initialize(ZkNodePathExistConstraint constraint) {}

  public boolean isValid(String path, ConstraintValidatorContext context) {
    return (zookeeperUtils.getNodePathStat(path) != null);
  }
}
