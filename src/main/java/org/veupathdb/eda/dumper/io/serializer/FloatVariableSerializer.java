package org.veupathdb.eda.dumper.io.serializer;

import java.nio.ByteBuffer;

public class FloatVariableSerializer implements VariableSerializer<Float,Float> {

  @Override
  public byte[] varValueToBytes(Float varValue) {
    return ByteBuffer.allocate(Float.BYTES).putFloat(varValue).array();
  }

  @Override
  public Float varValueFromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getFloat();
  }

  @Override
  public int varValueByteLength() {
    return Float.BYTES;
  }

  @Override
  public Float varValueFromString(String s) {
    return Float.parseFloat(s);
  }
}
