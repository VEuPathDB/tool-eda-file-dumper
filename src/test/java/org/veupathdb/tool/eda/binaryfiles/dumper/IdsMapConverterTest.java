package org.veupathdb.tool.eda.binaryfiles.dumper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.eda.binaryfiles.dumper.IdsMap;
import org.veupathdb.eda.binaryfiles.dumper.IdsMapConverter;

import java.util.List;

public class IdsMapConverterTest {

  @Test
  public void testToAndFromBytes() {
    IdsMapConverter idsMapConverter = new IdsMapConverter(2);
    List<String> ancestorIds = List.of("Ma", "GrandMa");
    IdsMap idsMap = new IdsMap(3L, "Me", ancestorIds);
    byte[] bytes = idsMapConverter.toBytes(idsMap);
    IdsMap converted = idsMapConverter.fromBytes(bytes);
    
    Assertions.assertTrue(idsMap.equals(converted), "Original: " + idsMap + " Converted: " + converted);
  }
}
