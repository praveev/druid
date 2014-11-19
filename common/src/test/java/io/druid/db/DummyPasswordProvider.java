package io.druid.db;

import java.util.Map;

import io.druid.common.config.PasswordProvider;

public class DummyPasswordProvider implements PasswordProvider {

  private String pwd;

  @Override
  public void init(Map<String, String> config)
  {
    this.pwd = config.get("pwd");
  }

  @Override
  public String getPassword()
  {
    return pwd;
  }
}
