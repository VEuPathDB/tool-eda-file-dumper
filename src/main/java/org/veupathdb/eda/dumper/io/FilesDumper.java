package org.veupathdb.eda.dumper.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.veupathdb.service.eda.ss.model.tabular.TabularResponses.ResultConsumer;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.converter.BinarySerializer;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdSerializer;

public interface FilesDumper extends ResultConsumer, AutoCloseable {
  
  public static final int BYTES_RESERVED_FOR_ID_STRING = 30;
  
  default <T> BinaryValueWriter<VariableValueIdPair<T>> getVarIdPairBinaryWriter(File file, ValueConverter<T> converter) {
    try {
      final FileOutputStream outStream = new FileOutputStream(file);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      final BinarySerializer<VariableValueIdPair<T>> serializer = new ValueWithIdSerializer<T>(converter);
      return new BinaryValueWriter<VariableValueIdPair<T>>(bufStream, serializer);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  }

  // no additional methods; just want to join these two interfaces

}
