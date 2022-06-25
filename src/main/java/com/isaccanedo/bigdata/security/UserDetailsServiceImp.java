package com.isaccanedo.bigdata.security;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import com.isaccanedo.bigdata.config.WebSecurityConfig;
import com.isaccanedo.bigdata.model.User;
import com.isaccanedo.bigdata.utils.CommonUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.security.core.userdetails.User.UserBuilder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.ResourceUtils;

@Log4j2
public class UserDetailsServiceImp implements UserDetailsService {

  private ScheduledExecutorService securityFileChecker;
  private ArrayList<User> userList = new ArrayList<>();

  public UserDetailsServiceImp(
      boolean checkSecurity, int checkInitDelay, int checkSecurityInterval) {
    if (checkSecurity) {
      securityFileChecker =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder().setNameFormat("securityFileChecker").build());
      securityFileChecker.scheduleWithFixedDelay(
          new SecurityFileCheckerRunnable(),
          checkInitDelay,
          checkSecurityInterval,
          TimeUnit.SECONDS);
      userList = fetchUserListFromSecurtiyFile();
    }
  }

  @Override
  public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
    User user = findUserByUsername(username);

    UserBuilder builder;
    if (user != null) {
      builder = org.springframework.security.core.userdetails.User.withUsername(username);
      builder.password(user.getPassword());
      builder.roles(user.getRole());
    } else {
      throw new UsernameNotFoundException("User not found.");
    }

    return builder.build();
  }

  private User findUserByUsername(String username) {
    for (User user : userList) {
      if (username.equals(user.getUsername())) {
        return user;
      }
    }
    return null;
  }

  private ArrayList<User> fetchUserListFromSecurtiyFile() {
    String securityFilePath = WebSecurityConfig.SECURITY_FILE_PATH;

    try {
      Resource resource = new ClassPathResource(securityFilePath);
      File file = resource.getFile();

      HashMap<Object, Object> accounts = CommonUtils.yamlParse(file);
      userList.clear();
      accounts.forEach(
          (key, value) -> {
            String username = (String) key;
            Map<String, String> userInfo = (Map) value;
            userList.add(new User(username, userInfo.get("password"), userInfo.get("role")));
          });
    } catch (IOException ioException) {
      log.error("Security file process exception.", ioException);
    }

    return userList;
  }

  private class SecurityFileCheckerRunnable implements Runnable {

    @Override
    public void run() {
      try {
        userList = fetchUserListFromSecurtiyFile();
      } catch (Throwable t) {
        log.error("Uncaught exception in SecurityFileChecker thread", t);
      }
    }
  }
}
