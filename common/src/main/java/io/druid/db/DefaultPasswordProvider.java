package io.druid.db;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;


public class DefaultPasswordProvider implements PasswordProvider
{

  public final static String KEY_PASSWORD = "password";

  private final String password;

  @JsonCreator
  public DefaultPasswordProvider(Object spec)
  {
    password = convertValue(spec);
  }

  private static String convertValue(Object spec) {
    if(spec instanceof String) {
      return spec.toString();
    } else if(spec instanceof Map && ((Map<String,String>)spec).containsKey(KEY_PASSWORD)) {
      return ((Map<String,String>)spec).get(KEY_PASSWORD);
    } else {
      throw new ISE("spec must be of type String or a Map<String,String> with having " + KEY_PASSWORD);
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
