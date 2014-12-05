package io.druid.db;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;


public class DefaultPasswordProvider implements PasswordProvider
{

  public final static String PASSWORD_KEY = "password";

  private final String password;

  @JsonCreator
  public DefaultPasswordProvider(Object spec)
  {
    password = convertValue(spec);
  }

  private static String convertValue(Object spec) {
    if(spec instanceof String) {
      return spec.toString();
    } else if(spec instanceof Map && ((Map)spec).containsKey(PASSWORD_KEY)) {
      return ((Map)spec).get(PASSWORD_KEY).toString();
    } else {
      throw new ISE("spec must be of type String or a Map<String,String> with having " + PASSWORD_KEY);
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
