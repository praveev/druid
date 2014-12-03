package io.druid.db;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;

import io.druid.common.config.PasswordProvider;

public class DefaultPasswordProvider implements PasswordProvider
{

  private final String password;

  @JsonCreator
  public DefaultPasswordProvider(Object spec)
  {
    password = convertValue(spec);
  }

  private static String convertValue(Object spec) {
    if(spec instanceof String) {
      return spec.toString();
    } else if(spec instanceof Map) {
      return ((Map<String,String>)spec).get("password");
    } else {
      throw new ISE("spec must be of type String or a Map");
    }
  }

  @Override
  @JsonProperty
  public String getPassword()
  {
    return password;
  }

  @Override
  public String toString() {
    return this.getClass().getCanonicalName();
  }
}
