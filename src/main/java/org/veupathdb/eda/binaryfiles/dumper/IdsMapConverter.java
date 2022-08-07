package org.veupathdb.eda.binaryfiles.dumper;

import org.veupathdb.service.eda.ss.model.variable.binary.BinaryDeserializer;
import org.veupathdb.service.eda.ss.model.variable.binary.BinarySerializer;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IdsMapConverter implements BinarySerializer<IdsMap>, BinaryDeserializer<IdsMap> {
  
  private int numAncestors;
  private StringValueConverter idStringConverter;
  private LongValueConverter idIndexConverter;
  
  public IdsMapConverter(int numAncestors) {
    this.numAncestors = numAncestors;
    this.idStringConverter = new StringValueConverter(FilesDumper.BYTES_RESERVED_FOR_ID_STRING);
    this.idIndexConverter = new LongValueConverter();
  }

  @Override
  public byte[] toBytes(IdsMap idsMap) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(numBytes());
    byteBuffer.putLong(idsMap.getIdIndex());
    byteBuffer.put(idStringConverter.toBytes(idsMap.getEntityId()));
    for (String ancestorId : idsMap.getAncestorIds()) 
      byteBuffer.put(idStringConverter.toBytes(ancestorId));     
    return byteBuffer.array();
  }

  @Override
  public int numBytes() {
    // idIndex + id_string + ancestor_id_strings
    return Long.BYTES + FilesDumper.BYTES_RESERVED_FOR_ID_STRING * (1 + numAncestors);
  }

  @Override
  public IdsMap fromBytes(byte[] bytes) {
    return fromBytes(bytes, 0);
  }

  @Override
  public IdsMap fromBytes(byte[] bytes, int offset) {
    Long idIndex = idIndexConverter.fromBytes(bytes, offset);
    offset += Long.SIZE;
    String entityId = idStringConverter.fromBytes(bytes, offset);
    List<String> ancestors = new ArrayList<String>();
    for (int i=0; i<numAncestors; i++) {
      offset += FilesDumper.BYTES_RESERVED_FOR_ID_STRING;
      String ancestorId = idStringConverter.fromBytes(bytes, offset);
      ancestors.add(ancestorId);
    }
    return new IdsMap(idIndex, entityId, ancestors);
  }

  @Override
  public IdsMap fromBytes(ByteBuffer bytes) {
    return fromBytes(bytes.array());
  }

}

