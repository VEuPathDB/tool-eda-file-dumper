package org.veupathdb.eda.binaryfiles.dumper;

import java.io.BufferedOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.List;

import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.tabular.TabularResponses.ResultConsumer;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.*;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager.Operation;

public interface FilesDumper extends ResultConsumer, AutoCloseable {

  default <T> BinaryValueWriter<VariableValueIdPair<T>> getVarAndIdBinaryWriter(File file, BinaryConverter<T> converter) {
    try {
      final FileOutputStream outStream = new FileOutputStream(file);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      final BinarySerializer<VariableValueIdPair<T>> serializer = new ValueWithIdSerializer<T>(converter);
      return new BinaryValueWriter<>(bufStream, serializer);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  }

  default BinaryValueWriter<RecordIdValues> getIdsMapWriter(BinaryFilesManager bfm, Study study, Entity entity, RecordIdValuesConverter idsMapSerializer) {
    final File idMapFile  = bfm.getIdMapFile(study, entity, Operation.WRITE).toFile();
    try {
      final FileOutputStream outStream = new FileOutputStream(idMapFile);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      return new BinaryValueWriter<>(bufStream, idsMapSerializer);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  }
  
  default BinaryValueWriter<List<Long>> getAncestorsWriter(BinaryFilesManager bfm, Study study, Entity entity) {
    final File ancestorsFile  = bfm.getAncestorFile(study, entity, Operation.WRITE).toFile();
    ListConverter<Long> converter =
        new ListConverter<Long>(new LongValueConverter(), entity.getAncestorEntities().size() + 1);
    try {
      final FileOutputStream outStream = new FileOutputStream(ancestorsFile);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      return new BinaryValueWriter<List<Long>>(bufStream, converter);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  }

  default BinaryValueWriter<RecordIdValues> getIdsMapWriter(File file, RecordIdValuesConverter converter) {
    try {
      final FileOutputStream outStream = new FileOutputStream(file);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      return new BinaryValueWriter<>(bufStream, converter);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  } 
}
