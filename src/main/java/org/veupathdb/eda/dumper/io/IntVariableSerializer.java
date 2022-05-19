package org.veupathdb.eda.dumper.io;

import java.nio.ByteBuffer;

public class IntVariableSerializer extends VariableSerializer<Integer> {

  @Override
  byte[] varValueToBytes(Integer varValue) {
    return ByteBuffer.allocate(INT_LENGTH).putInt(varValue).array();
  }

  @Override
  Integer varValueFromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  @Override
  int varValueByteLength() {
    return INT_LENGTH;
  }
}
