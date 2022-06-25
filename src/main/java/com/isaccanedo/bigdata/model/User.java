package com.isaccanedo.bigdata.model;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class User {

  @NotNull(message = "Username can not be null.")
  @NotBlank(message = "Username can not be blank.")
  private String username;

  @NotNull(message = "Password can not be null.")
  @NotBlank(message = "Password can not be blank.")
  private String password;

  @NotNull(message = "Role can not be null.")
  @NotBlank(message = "Role can not be blank.")
  private String role;
}
