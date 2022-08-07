package org.veupathdb.eda.binaryfiles.dumper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager.Operation;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdDeserializer;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;

/**
 * Dump ID files for an entity that has exactly one ancestor
 * 
 * reads from:
 *  - tabular stream
 *  - parent idMap
 * writes to
 *  - idMap
 *  - ancestor 
 *  
 * @author sfischer
 *
 */
public class IdFilesDumperOneAncestor implements FilesDumper {
  private static final int RECORDS_PER_BUFFER = 100;
  private final static int ID_COLUMN_INDEX = 0;  // the position in tabular stream of entity ID
  private final static int PARENT_ID_COLUMN_INDEX = 1; // the position in the tabular stream of the parent's ID

  private final ValueWithIdDeserializer<String> _parentIdMapDeserializer;
  private final DualBufferBinaryRecordReader _parentIdMapReader;
  private final BinaryValueWriter<VariableValueIdPair<String>> _idMapWriter;
  private final BinaryValueWriter<List<Long>> _ancestorsWriter;
  boolean _firstRow = true;
  
  private VariableValueIdPair<String> _currentParentIdMapRow = new VariableValueIdPair<String>(-1L, "non-existent ID");

  private AtomicLong _idIndex = new AtomicLong(0);

  public IdFilesDumperOneAncestor(BinaryFilesManager bfm, Study study, Entity entity, Entity parentEntity) {

    // create input readers
    _parentIdMapDeserializer = new ValueWithIdDeserializer<String>(new StringValueConverter(BYTES_RESERVED_FOR_ID_STRING));
    try {   
      _parentIdMapReader = 
          new DualBufferBinaryRecordReader(
              bfm.getIdMapFile(study, parentEntity, Operation.READ), 
              _parentIdMapDeserializer.numBytes(), 
              RECORDS_PER_BUFFER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    // create writers
    final File idMapFile  = bfm.getIdMapFile(study, entity, Operation.WRITE).toFile();
    _idMapWriter = getVarAndIdBinaryWriter(idMapFile, new StringValueConverter(BYTES_RESERVED_FOR_ID_STRING));

    final File idAncestorFile  = bfm.getAncestorFile(study, entity, Operation.WRITE).toFile();
    _ancestorsWriter = getAncestorsWriter(idAncestorFile, 
        new ListConverter<Long>(new LongValueConverter(), entity.getAncestorEntities().size() + 1));
  }
  
  @Override
  public void consumeRow(List<String> row) throws IOException {
    if (_firstRow) { _firstRow = false; return; } // skip header
    
    String curStringId = row.get(ID_COLUMN_INDEX);
    Long curIdIndex = _idIndex.getAndIncrement();
    
    // write out idMap row
    VariableValueIdPair<String> idMap = new VariableValueIdPair<>(curIdIndex, curStringId);
    _idMapWriter.writeValue(idMap);
        
   // write out ancestors row
   advanceParentStreams(row.get(PARENT_ID_COLUMN_INDEX));
   List<Long> ancestorsRow = new ArrayList<Long>();
    ancestorsRow.add(curIdIndex);
    ancestorsRow.add(Long.valueOf(_currentParentIdMapRow.getIdIndex()));
    _ancestorsWriter.writeValue(ancestorsRow);
  }
  
  @Override
  public void close() throws Exception {
    _parentIdMapReader.close();
    _idMapWriter.close();
    _ancestorsWriter.close();
  }
  
  /* advance parent streams to our current ID (if needed)
   *   - read parent id map file to compare parent string ID with what we got from the tabular stream
   *   - keep reading until they match
   *   - globally remember parent id row
   */
  private void advanceParentStreams(String parentIdString) {
    
    while (!parentIdString.equals(_currentParentIdMapRow.getValue())) {
      
      // remember current parent ID string
      VariableValueIdPair<String> parentIdMapRow = 
          _parentIdMapReader
          .next()
          .map(_parentIdMapDeserializer::fromBytes)
          .orElseThrow(() -> new RuntimeException("Unexpected end of parent id map file"));
      _currentParentIdMapRow = parentIdMapRow;
    }
  }

}
