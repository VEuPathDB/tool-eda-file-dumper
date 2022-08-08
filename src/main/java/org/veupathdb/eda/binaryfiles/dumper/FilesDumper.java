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
import org.veupathdb.service.eda.ss.model.variable.binary.BinarySerializer;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdSerializer;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager.Operation;

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
  
  default BinaryValueWriter<IdsMap> getIdsMapWriter(BinaryFilesManager bfm, Study study, Entity entity) {
    final File idMapFile  = bfm.getIdMapFile(study, entity, Operation.WRITE).toFile();
    IdsMapConverter idsMapSerializer = new IdsMapConverter(entity.getAncestorEntities().size());
    try {
      final FileOutputStream outStream = new FileOutputStream(idMapFile);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      return new BinaryValueWriter<IdsMap>(bufStream, idsMapSerializer);
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

  default BinaryValueWriter<IdsMap> getIdsMapWriter2(File file, IdsMapConverter converter) {
    try {
      final FileOutputStream outStream = new FileOutputStream(file);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      return new BinaryValueWriter<IdsMap>(bufStream, converter);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
  } 
}
