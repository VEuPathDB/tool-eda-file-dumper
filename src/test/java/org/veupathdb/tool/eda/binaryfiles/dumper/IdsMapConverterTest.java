package org.veupathdb.tool.eda.binaryfiles.dumper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.binary.RecordIdValues;
import org.veupathdb.service.eda.ss.model.variable.binary.RecordIdValuesConverter;

import java.util.List;

public class IdsMapConverterTest {

  @Test
  public void testToAndFromBytes() {
    RecordIdValuesConverter idsMapConverter = new RecordIdValuesConverter(List.of(6, 11), 6);
    List<String> ancestorIds = List.of("Ma", "GrandMa");
    RecordIdValues idsMap = new RecordIdValues(3L, "Me", ancestorIds);
    byte[] bytes = idsMapConverter.toBytes(idsMap);
    RecordIdValues converted = idsMapConverter.fromBytes(bytes);
    
    Assertions.assertTrue(idsMap.equals(converted), "Original: " + idsMap + " Converted: " + converted);
  }
}
