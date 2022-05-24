package org.veupathdb.eda.dumper.io.serializer;

/**
 * Base interface for serializers
 *
 * @param <S> type of object being represented (value eventually being used in filters)
 * @param <T> type of intermediate value directly translatable to byte[]
 */
public interface VariableSerializer<S,T> {

  S varValueFromString(String s);

  byte[] varValueToBytes(T varValue);

  T varValueFromBytes(byte[] bytes);

  int varValueByteLength();

}