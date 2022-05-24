package org.veupathdb.eda.dumper.io.serializer;

import java.nio.ByteBuffer;

import org.gusdb.fgputil.ComparableLocalDateTime;
import org.gusdb.fgputil.FormatUtil;

public class DateVariableSerializer implements VariableSerializer<ComparableLocalDateTime, Long> {

  @Override
  public byte[] varValueToBytes(Long varValue) {
    return ByteBuffer.allocate(Long.BYTES).putLong(varValue).array();
  }

  @Override
  public Long varValueFromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }

  @Override
  public int varValueByteLength() {
    return Long.BYTES;
  }

  @Override
  public ComparableLocalDateTime varValueFromString(String s) {
    return new ComparableLocalDateTime(FormatUtil.parseDateTime(s));
  }

}
