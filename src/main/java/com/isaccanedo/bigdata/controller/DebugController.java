package com.isaccanedo.bigdata.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.GroupTopicPartition;
import com.isaccanedo.bigdata.componet.OffsetStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

/** Created by gnuhpc on 2017/7/16. */
@RequestMapping("/debug")
@RestController
@ApiIgnore
public class DebugController {

  @Autowired ApplicationContext appContext;

  @Autowired OffsetStorage offsetStorage;

  @RequestMapping("/beans")
  public Map<String, String[]> beans(@RequestParam(required = false) String q) {
    Map<String, String[]> retMap = new HashMap<>();

    String[] retArray =
        Arrays.stream(appContext.getBeanDefinitionNames())
            .filter(
                beanName ->
                    (q == null || q.length() == 0)
                        || beanName.toLowerCase().contains(q.trim().toLowerCase()))
            .toArray(String[]::new);

    retMap.put("beans", retArray);
    return retMap;
  }

  @RequestMapping("/offsets")
  public Map<String, Map<GroupTopicPartition, OffsetAndMetadata>> offsets() {
    return offsetStorage.getMap();
  }
}
