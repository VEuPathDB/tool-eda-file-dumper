package org.veupathdb.tool.eda.binaryfiles.dumper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.binary.RecordIdValues;
import org.veupathdb.service.eda.ss.model.variable.binary.RecordIdValuesConverter;

import java.math.BigDecimal;
import java.math.RoundingMode;
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

  @Test
  public void test() {
//    BigDecimal bd = BigDecimal.valueOf(111.123456);
    Double d = Double.valueOf("-111.920029");
    BigDecimal bd = BigDecimal.valueOf(d);
    System.out.println(bd.scale());
    bd = bd.setScale(4, RoundingMode.HALF_UP);
    System.out.println(bd.toPlainString());
  }
}
