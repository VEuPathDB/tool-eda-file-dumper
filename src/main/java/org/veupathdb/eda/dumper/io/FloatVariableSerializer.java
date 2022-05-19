package org.veupathdb.eda.dumper.io;

import java.nio.ByteBuffer;

public class FloatVariableSerializer extends VariableSerializer<Float> {
  private static int FLOAT_LENGTH = 4;

  @Override
  byte[] varValueToBytes(Float varValue) {
    return ByteBuffer.allocate(FLOAT_LENGTH).putFloat(varValue).array();
  }

  @Override
  Float varValueFromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getFloat();
  }

  @Override
  int varValueByteLength() {
    return FLOAT_LENGTH;
  }
}
