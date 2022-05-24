package org.veupathdb.eda.dumper.io.serializer;

import java.nio.ByteBuffer;

public class IntVariableSerializer implements VariableSerializer<Integer,Integer> {

  @Override
  public byte[] varValueToBytes(Integer varValue) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(varValue).array();
  }

  @Override
  public Integer varValueFromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  @Override
  public int varValueByteLength() {
    return Integer.BYTES;
  }

  @Override
  public Integer varValueFromString(String s) {
    return Integer.parseInt(s);
  }
}
