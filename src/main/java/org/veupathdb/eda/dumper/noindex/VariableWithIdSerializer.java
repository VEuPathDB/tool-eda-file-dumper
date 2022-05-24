package org.veupathdb.eda.dumper.noindex;

import java.nio.ByteBuffer;

import org.veupathdb.eda.dumper.io.serializer.VariableSerializer;

public class VariableWithIdSerializer<S,T> {

  private final VariableSerializer<S,T> _valueSerializer;

  public VariableWithIdSerializer(VariableSerializer<S,T> valueSerializer) {
    _valueSerializer = valueSerializer;
  }

  /**
   * Converts a variable to an array of bytes. The first 4 bytes are written as an integer variable identifier and the
   * next N are read as the variable value.
   * @param variable to convert to bytes
   * @return Deserialized variable object
   */
  public byte[] convertToBytes(Variable<T> variable) {
    final int bufferSize = totalBytesNeeded();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    byteBuffer.putInt(variable.getId());
    byteBuffer.put(_valueSerializer.varValueToBytes(variable.getValue()));
    return byteBuffer.array();
  }

  /**
   * Converts an array of bytes to a variable. The first 4 bytes are read as an integer and the next N are read as
   * the variable value.
   * @param bytes to convert to variable
   * @return Deserialized variable object
   */
  public Variable<T> covertFromBytes(byte[] bytes) {
    if (bytes.length != totalBytesNeeded()) {
      throw new IllegalArgumentException("Expected byte array of size, " + totalBytesNeeded() + " but found: "
          + bytes.length);
    }
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final int varId = byteBuffer.getInt();
    final byte[] varValueBytes = new byte[_valueSerializer.varValueByteLength()];
    byteBuffer.get(varValueBytes);
    final T varValue = _valueSerializer.varValueFromBytes(varValueBytes);
    return new Variable<T>(varId, varValue);
  }

  public int totalBytesNeeded() {
    // Keep 4 for the variable identifier and the rest for the value.
    return Integer.BYTES + _valueSerializer.varValueByteLength();
  }

}

