package com.isaccanedo.bigdata.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import com.isaccanedo.bigdata.config.WebSecurityConfig;
import com.isaccanedo.bigdata.constant.GeneralResponseState;
import com.isaccanedo.bigdata.model.GeneralResponse;
import com.isaccanedo.bigdata.model.User;
import com.isaccanedo.bigdata.utils.CommonUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

@Getter
@Setter
@Log4j2
@Service
public class UserService {

  private HashMap<Object, Object> accounts;

  public List<String> listUser() {
    List<String> userList = new ArrayList<>();
    try {
      Resource resource = new ClassPathResource(WebSecurityConfig.SECURITY_FILE_PATH);
      File file = resource.getFile();
      accounts = CommonUtils.yamlParse(file);
      accounts.forEach(
          (username, value) -> {
            userList.add((String) username);
          });
    } catch (IOException ioException) {
      log.error("Failed to get user list. Reason : " + ioException.getLocalizedMessage());
    }

    return userList;
  }

  public GeneralResponse addUser(User user) {
    String username = user.getUsername();
    try {
      boolean exist = checkUserExist(username);
      if (!exist) {
        return saveUserInfo(user);
      } else {
        log.info("Failed to add user. Reason : User " + username + " already exists.");
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg("Failed to add user. Reason : User " + username + " already exists.")
            .build();
      }
    } catch (IOException ioException) {
      log.error(
          "Failed to add user " + username + ". Reason : " + ioException.getLocalizedMessage());
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg("Failed to add user " + username + ". Reason : " + ioException.getLocalizedMessage())
          .build();
    }
  }

  public GeneralResponse modifyUser(User user) {
    String username = user.getUsername();
    try {
      boolean exist = checkUserExist(username);
      if (exist) {
        return saveUserInfo(user);
      } else {
        log.info("Failed to modify user. Reason : User " + username + " does not exist.");
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg("Failed to modify user. Reason : User " + username + " does not exist.")
            .build();
      }
    } catch (IOException ioException) {
      log.error(
          "Failed to modify user " + username + ". Reason : " + ioException.getLocalizedMessage());
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg(
              "Failed to modify user "
                  + username
                  + ". Reason : "
                  + ioException.getLocalizedMessage())
          .build();
    }
  }

  public GeneralResponse delUser(String username) {
    try {
      boolean exist = checkUserExist(username);
      if (exist) {
        accounts.remove(username);
        CommonUtils.yamlWrite(WebSecurityConfig.SECURITY_FILE_PATH, accounts);
        return GeneralResponse.builder()
            .state(GeneralResponseState.success)
            .msg("Delete user " + username + " successfully.")
            .build();
      } else {
        log.info("Failed to delete user. Reason : User " + username + " does not exist.");
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg("Failed to delete user. Reason : User " + username + " does not exist.")
            .build();
      }
    } catch (IOException ioException) {
      log.error(
          "Failed to delete user " + username + ". Reason : " + ioException.getLocalizedMessage());
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg(
              "Failed to delete user "
                  + username
                  + ". Reason : "
                  + ioException.getLocalizedMessage())
          .build();
    }
  }

  public boolean checkUserExist(String username) throws IOException {
    Resource resource = new ClassPathResource(WebSecurityConfig.SECURITY_FILE_PATH);
    File file = resource.getFile();
    accounts = CommonUtils.yamlParse(file);
    if (accounts.containsKey(username)) {
      return true;
    }
    return false;
  }

  public GeneralResponse saveUserInfo(User user) throws IOException {
    String username = user.getUsername();
    String encodedPassword = CommonUtils.encode(user.getPassword());
    HashMap<String, String> userInfo = new HashMap<>();

    userInfo.put("password", encodedPassword);
    userInfo.put("role", user.getRole());
    accounts.put(username, userInfo);
    Resource resource = new ClassPathResource(WebSecurityConfig.SECURITY_FILE_PATH);
    File file = resource.getFile();
    CommonUtils.yamlWrite(file, accounts);
    return GeneralResponse.builder()
        .state(GeneralResponseState.success)
        .msg("Save user " + username + " info successfully.")
        .build();
  }
}
