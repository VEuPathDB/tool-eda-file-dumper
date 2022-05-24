package org.veupathdb.eda.dumper.io.serializer;

import org.gusdb.fgputil.FormatUtil;

public class StringVariableSerializer implements VariableSerializer<String, String> {

  private final int _numBytes;

  public StringVariableSerializer(int numBytes) {
    _numBytes = numBytes;
  }

  @Override
  public byte[] varValueToBytes(String varValue) {
    return FormatUtil.stringToPaddedBinary(varValue, _numBytes);
  }

  @Override
  public String varValueFromBytes(byte[] bytes) {
    return FormatUtil.paddedBinaryToString(bytes);
  }

  @Override
  public int varValueByteLength() {
    return _numBytes;
  }

  @Override
  public String varValueFromString(String s) {
    return s;
  }

}
