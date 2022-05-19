package org.veupathdb.eda.dumper.io;

import java.nio.ByteBuffer;

public class FloatVariableSerializer extends VariableSerializer<Float> {
  private static int FLOAT_LENGTH = 4;

  @Override
  byte[] valueToBytes(Float varValue) {
    return ByteBuffer.allocate(FLOAT_LENGTH).putFloat(varValue).array();
  }

  @Override
  Float valueFromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getFloat();
  }

  @Override
  Float valueFromByteBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getFloat();
  }

  @Override
  int valueLength() {
    return FLOAT_LENGTH;
  }
}
