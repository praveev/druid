package io.druid.db;

import java.util.Map;

import io.druid.common.config.PasswordProvider;

public class DummyPasswordProvider implements PasswordProvider {
  
  @Override
  public void init(Map<String, String> config) {}

  @Override
  public String getPassword()
  {
    return "nothing";
  }
}
