package io.druid.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;


public class JsonTestUtils
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();


  public static <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static byte [] jsonWriteReadWrite(Object object)
  {
    try {
      return jsonMapper.writeValueAsBytes(jsonMapper.readValue(jsonMapper.writeValueAsBytes(object), object.getClass()));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
