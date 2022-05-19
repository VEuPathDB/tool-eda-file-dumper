package org.veupathdb.eda.dumper.io;

import org.veupathdb.eda.dumper.model.VariableDataPoint;

import java.nio.ByteBuffer;

public abstract class VariableSerializer<T> {
  protected static int INT_LENGTH = 4;

  /**
   * Converts a variable to an array of bytes. The first 4 bytes are written as an integer variable identifier and the
   * next N are read as the variable value.
   * @param variable to convert to bytes
   * @return Deserialized variable object
   */
  public byte[] convertToBytes(VariableDataPoint<T> variable) {
    final int bufferSize = totalBytesNeeded();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    byteBuffer.putInt(variable.getId());
    byteBuffer.put(valueToBytes(variable.getValue()));
    return byteBuffer.array();
  }

  /**
   * Converts an array of bytes to a variable. The first 4 bytes are read as an integer and the next N are read as
   * the variable value.
   * @param bytes to convert to variable
   * @return Deserialized variable object
   */
  public VariableDataPoint<T> covertFromBytes(byte[] bytes) {
    if (bytes.length != totalBytesNeeded()) {
      throw new IllegalArgumentException("Expected byte array of size, " + totalBytesNeeded() + " but found: "
          + bytes.length);
    }
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final int varId = byteBuffer.getInt();
    final byte[] varValueBytes = new byte[valueLength()];
    byteBuffer.get(varValueBytes);
    final T varValue = valueFromBytes(varValueBytes);
    return new VariableDataPoint<>(varId, varValue);
  }

  public VariableDataPoint<T> convertFromBuffer(ByteBuffer bytes) {
    final int varId = bytes.getInt();
    final T varValue = valueFromByteBuffer(bytes);
    return new VariableDataPoint<>(varId, varValue);
  }

  public int totalBytesNeeded() {
    // Keep 4 for the variable identifier and the rest for the value.
    return INT_LENGTH + valueLength();
  }

  abstract byte[] valueToBytes(T varValue);

  abstract T valueFromBytes(byte[] bytes);

  abstract T valueFromByteBuffer(ByteBuffer byteBuffer);

  abstract int valueLength();
}
