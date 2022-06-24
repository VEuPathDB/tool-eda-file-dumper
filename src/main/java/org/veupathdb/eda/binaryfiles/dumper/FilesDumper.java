package org.veupathdb.eda.binaryfiles.dumper;

import java.io.BufferedOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.List;

import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.ss.model.tabular.TabularResponses.ResultConsumer;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.BinarySerializer;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdSerializer;

public interface FilesDumper extends ResultConsumer, AutoCloseable {
  
  public static final int BYTES_RESERVED_FOR_ID_STRING = 30;
  
  default <T> BinaryValueWriter<VariableValueIdPair<T>> getVarAndIdBinaryWriter(File file, BinaryConverter<T> converter) {
    try {
      final FileOutputStream outStream = new FileOutputStream(file);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      final BinarySerializer<VariableValueIdPair<T>> serializer = new ValueWithIdSerializer<T>(converter);
      return new BinaryValueWriter<VariableValueIdPair<T>>(bufStream, serializer);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  }
  
  default BinaryValueWriter<List<Long>> getAncestorsWriter(File file, ListConverter<Long> converter) {
    try {
      final FileOutputStream outStream = new FileOutputStream(file);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      return new BinaryValueWriter<List<Long>>(bufStream, converter);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  }


}
