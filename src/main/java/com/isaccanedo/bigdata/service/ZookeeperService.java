package com.isaccanedo.bigdata.service;

import com.google.common.net.HostAndPort;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import com.isaccanedo.bigdata.constant.ZkServerCommand;
import com.isaccanedo.bigdata.exception.ServiceNotAvailableException;
import com.isaccanedo.bigdata.model.ZkServerEnvironment;
import com.isaccanedo.bigdata.model.ZkServerStat;
import com.isaccanedo.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class ZookeeperService {
  @Lazy
  @Autowired private ZookeeperUtils zookeeperUtils;

  public Map<HostAndPort, ZkServerStat> stat() {
    List<HostAndPort> hostAndPortList = zookeeperUtils.getZookeeperConfig().getHostAndPort();
    Map<HostAndPort, ZkServerStat> result = new HashMap<>();
    for (int i = 0; i < hostAndPortList.size(); i++) {
      HostAndPort hp = hostAndPortList.get(i);
      try {
        result.put(
            hp,
            zookeeperUtils.parseStatResult(
                zookeeperUtils.executeCommand(
                    hp.getHostText(), hp.getPort(), ZkServerCommand.stat.toString())));
      } catch (ServiceNotAvailableException serviceNotAvailbleException) {
        log.warn(
            "Execute "
                + ZkServerCommand.stat.toString()
                + " command failed. Exception:"
                + serviceNotAvailbleException);
        result.put(
            hp,
            ZkServerStat.builder()
                .mode(serviceNotAvailbleException.getServiceState())
                .msg(serviceNotAvailbleException.getMessage())
                .build());
      }
    }
    return result;
  }

  public Map<HostAndPort, ZkServerEnvironment> environment() {
    List<HostAndPort> hostAndPortList = zookeeperUtils.getZookeeperConfig().getHostAndPort();
    Map<HostAndPort, ZkServerEnvironment> result = new HashMap<>();
    for (int i = 0; i < hostAndPortList.size(); i++) {
      HostAndPort hp = hostAndPortList.get(i);
      try {
        result.put(
            hp,
            zookeeperUtils.parseEnvResult(
                zookeeperUtils.executeCommand(
                    hp.getHostText(), hp.getPort(), ZkServerCommand.envi.toString())));
      } catch (ServiceNotAvailableException serviceNotAvailbleException) {
        log.warn(
            "Execute "
                + ZkServerCommand.envi.toString()
                + " command failed. Exception:"
                + serviceNotAvailbleException);
        ZkServerEnvironment zkServerEnvironment = new ZkServerEnvironment();
        zkServerEnvironment.add("mode", serviceNotAvailbleException.getServiceState().toString());
        zkServerEnvironment.add("msg", serviceNotAvailbleException.getMessage());
        result.put(hp, zkServerEnvironment);
      }
    }
    return result;
  }
}
