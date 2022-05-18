package org.veupathdb.eda.dumper.io;

import org.veupathdb.eda.dumper.model.Variable;

import java.nio.ByteBuffer;

public abstract class VariableSerializer<T> {

  public byte[] convertToBytes(Variable<T> variable) {
    final int bufferSize = 4 + varValueByteLength();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    byteBuffer.putInt(variable.getId());
    byteBuffer.put(varValueToBytes(variable.getValue()));
    return byteBuffer.array();
  }

  public Variable<T> covertFromBytes(byte[] bytes) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final int varId = byteBuffer.getInt();
    final byte[] varValueBytes = new byte[varValueByteLength()];
    byteBuffer.get(varValueBytes);
    final T varValue = varValueFromBytes(varValueBytes);
    return new Variable<T>(varId, varValue);
  }

  abstract byte[] varValueToBytes(T varValue);

  abstract T varValueFromBytes(byte[] bytes);

  abstract int varValueByteLength();
}
