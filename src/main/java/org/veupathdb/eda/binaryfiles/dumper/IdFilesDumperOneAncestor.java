package org.veupathdb.eda.binaryfiles.dumper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.variable.binary.*;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager.Operation;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

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
  private final DualBufferBinaryRecordReader<VariableValueIdPair<String>> _parentIdMapReader;
  private final BinaryValueWriter<RecordIdValues> _idsMapWriter;
  private final BinaryValueWriter<List<Long>> _ancestorsWriter;
  boolean _firstRow = true;
  
  private VariableValueIdPair<String> _currentParentIdMapRow = new VariableValueIdPair<String>(-1L, "non-existent ID");

  private AtomicLong _idIndex = new AtomicLong(0);

  public IdFilesDumperOneAncestor(BinaryFilesManager bfm, Study study, Entity entity, Entity parentEntity, Map<String, Integer> bytesReservedForIdByEntityId) {

    // create input readers
    int bytesReservedForParentId = bytesReservedForIdByEntityId.get(parentEntity.getId());
    _parentIdMapDeserializer = new ValueWithIdDeserializer<>(new StringValueConverter(bytesReservedForParentId));
    try {   
      _parentIdMapReader = 
          new DualBufferBinaryRecordReader<>(
              bfm.getIdMapFile(study, parentEntity, Operation.READ), 
              _parentIdMapDeserializer.numBytes(), 
              RECORDS_PER_BUFFER,
              _parentIdMapDeserializer::fromBytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    // create writers
    _idsMapWriter = getIdsMapWriter(bfm, study, entity, new RecordIdValuesConverter(entity, bytesReservedForIdByEntityId));
    _ancestorsWriter = getAncestorsWriter(bfm, study, entity);
  }
    
  @Override
  public void consumeRow(List<String> row) throws IOException {
    if (_firstRow) { _firstRow = false; return; } // skip header
    
    Long curIdIndex = _idIndex.getAndIncrement();
    String curStringId = row.get(ID_COLUMN_INDEX);
    List<String> ancestorIds = row.subList(1, row.size());
    
    // write out ids_map file row
    RecordIdValues idsMap = new RecordIdValues(curIdIndex, curStringId, ancestorIds);
    _idsMapWriter.writeValue(idsMap);
        
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
    _idsMapWriter.close();
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
          .orElseThrow(() -> new RuntimeException("Unexpected end of parent id map file"));
      _currentParentIdMapRow = parentIdMapRow;
    }
  }

}
