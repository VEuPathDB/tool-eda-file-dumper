package org.veupathdb.eda.dumper.io;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdDeserializer;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;

/**
 * reads from:
 *  - tabular stream
 *  - parent idMap
 *  - parent ancestor
 * writes to
 *  - idMap
 *  - ancestor 
 * @author sfischer
 *
 */
public class IdFilesDumper implements FilesDumper {
  private static final int RECORDS_PER_BUFFER = 100;

  private final ListConverter<Long> _parentAncestorConverter;
  private final ValueWithIdDeserializer<String> _parentIdMapDeserializer;
  private DualBufferBinaryRecordReader _parentAncestorReader;
  private DualBufferBinaryRecordReader _parentIdMapReader;
  private BinaryValueWriter<VariableValueIdPair<String>> _imfWriter;
  private AtomicLong _idIndex = new AtomicLong(0);


  public IdFilesDumper(BinaryFilesManager bfm, Study study, Entity entity, Entity parentEntity) {

    // create input readers
    _parentAncestorConverter = new ListConverter<>(new LongValueConverter(), entity.getAncestorEntities().size());
    _parentIdMapDeserializer = new ValueWithIdDeserializer<String>(new StringValueConverter(BYTES_RESERVED_FOR_ID_STRING));
    try {
      _parentAncestorReader = 
          new DualBufferBinaryRecordReader(
              bfm.getAncestorFile(study, parentEntity), 
              _parentAncestorConverter.numBytes(), 
              RECORDS_PER_BUFFER);
      
      _parentIdMapReader = 
          new DualBufferBinaryRecordReader(
              bfm.getIdMapFile(study, parentEntity), 
              _parentIdMapDeserializer.numBytes(), 
              RECORDS_PER_BUFFER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    final File imf  = bfm.getIdMapFile(study, entity).toFile();
    _imfWriter = getVarIdPairBinaryWriter(imf, new StringValueConverter(BYTES_RESERVED_FOR_ID_STRING));

  // TODO create and open relevant files for writing
  }
  
  @Override
  public void consumeRow(List<String> row) throws IOException {
    Optional<List<Long>> parentAncestorRow = _parentAncestorReader.next().map(_parentAncestorConverter::fromBytes);
    Optional<VariableValueIdPair<String>> parentIdMapRow = _parentIdMapReader.next().map(_parentIdMapDeserializer::fromBytes);

  
  }

  @Override
  public void close() throws Exception {
    _parentAncestorReader.close();
    _parentIdMapReader.close();
  }

}
