package org.veupathdb.eda.dumper.io;

import java.nio.ByteBuffer;

public class IntVariableSerializer extends VariableSerializer<Integer> {

  @Override
  byte[] valueToBytes(Integer varValue) {
    return ByteBuffer.allocate(INT_LENGTH).putInt(varValue).array();
  }

  @Override
  Integer valueFromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  @Override
  Integer valueFromByteBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getInt();
  }

  @Override
  int valueLength() {
    return INT_LENGTH;
  }
}
