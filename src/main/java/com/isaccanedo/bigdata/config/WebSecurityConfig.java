package com.isaccanedo.bigdata.config;

import com.isaccanedo.bigdata.security.BasicAuthenticationPoint;
import com.isaccanedo.bigdata.security.UserDetailsServiceImp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@Configuration
@EnableWebSecurity
@Lazy
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

  public static final String SECURITY_FILE_PATH = "security.yml";

  @Autowired private BasicAuthenticationPoint basicAuthenticationPoint;

  @Value("${server.security.check}")
  private boolean securityCheck;

  @Value("${server.security.checkInitDelay}")
  private int checkInitDelay;

  @Value("${server.security.checkSecurityInterval}")
  private int checkSecurityInterval;

  @Bean
  public UserDetailsService userDetailsService() {
    return new UserDetailsServiceImp(securityCheck, checkInitDelay, checkSecurityInterval);
  }

  @Bean
  public BCryptPasswordEncoder passwordEncoder() {
    return new BCryptPasswordEncoder();
  }

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.csrf().disable();
    if (securityCheck) {
      http.authorizeRequests()
          .antMatchers("/api", "/swagger-ui.html", "/webjars/**", "/swagger-resources/**", "/v2/**")
          .permitAll()
          .antMatchers(HttpMethod.GET, "/**")
          .permitAll()
          .anyRequest()
          .authenticated();
      http.httpBasic().authenticationEntryPoint(basicAuthenticationPoint);
      http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);
    } else {
      http.authorizeRequests().antMatchers("/**").permitAll().anyRequest().authenticated();
    }
  }

  @Autowired
  public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
    auth.userDetailsService(userDetailsService()).passwordEncoder(passwordEncoder());
  }
}
