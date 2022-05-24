package org.veupathdb.eda.dumper.io.serializer;

import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class SerializerFactory {

  // TODO: do two passes to determine max size for individual vars and store off as a metadata file
  private static final int MULTIVALUE_STRING_SIZE = 1000;
  private static final int SINGLE_VALUE_STRING_SIZE = 100;

  public VariableSerializer<?,?> getSerializer(VariableWithValues var) {

    if (var.getIsMultiValued())
      return new StringVariableSerializer(MULTIVALUE_STRING_SIZE);

    switch (var.getType()) {
      case DATE: return new DateVariableSerializer();
      case INTEGER: return new IntVariableSerializer();
      case LONGITUDE: // pass through
      case NUMBER: return new FloatVariableSerializer();
      case STRING: return new StringVariableSerializer(SINGLE_VALUE_STRING_SIZE);
      default: throw new IllegalArgumentException();
    }
  }
}
