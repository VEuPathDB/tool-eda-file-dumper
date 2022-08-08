package org.veupathdb.eda.binaryfiles.dumper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager.Operation;

/**
 * Dump ID files for an entity that has more than one ancestor
 * 
 * reads from:
 *  - tabular stream
 *  - parent ids_map  (needed to map parent entity_id to parent idIndex)
 *  - parent ancestors
 * writes to
 *  - ids_map   ([idIndex, entityId, ancestorIds])
 *  - ancestors 
 *  
 * @author sfischer
 *
 */
public class IdsMapFileDumper implements FilesDumper {
  private static final int RECORDS_PER_BUFFER = 100;
  private final static int ID_COLUMN_INDEX = 0;  // the position in tabular stream of entity ID
  private final static int PARENT_ID_COLUMN_INDEX = 1; // the position in the tabular stream of the parent's ID

  private final ListConverter<Long> _parentAncestorsDeserializer;
  private final DualBufferBinaryRecordReader _parentAncestorsReader;
  private final IdsMapConverter _parentIdsMapDeserializer;
  private final DualBufferBinaryRecordReader _parentIdsMapReader;
  private final BinaryValueWriter<IdsMap> _idsMapWriter;
  private final BinaryValueWriter<List<Long>> _ancestorsWriter;
  boolean _firstRow = true;
  
  private List<Long> _currentParentAncestorRow;
  private String _currentParentIdString = "initialized to a non-existent ID";

  private AtomicLong _idIndex = new AtomicLong(0);

  public IdsMapFileDumper(BinaryFilesManager bfm, Study study, Entity entity, Entity parentEntity) {
    
    int entityAncestorsCount = entity.getAncestorEntities().size();
    int parentEntityAncestorsCount = parentEntity.getAncestorEntities().size();
    
    if (entityAncestorsCount < 1) 
      throw new IllegalArgumentException("Entity must have at least one ancestor.");
    if (parentEntityAncestorsCount != entityAncestorsCount)
      throw new IllegalArgumentException("Parent entity must have one fewer ancestors than entity.");

    // create input readers
    _parentAncestorsDeserializer = new ListConverter<>(new LongValueConverter(), entity.getAncestorEntities().size());
    _parentIdsMapDeserializer = new IdsMapConverter(parentEntityAncestorsCount);
    try {
      
      _parentAncestorsReader =
          new DualBufferBinaryRecordReader(
              bfm.getAncestorFile(study, parentEntity, Operation.READ), 
              _parentAncestorsDeserializer.numBytes(), 
              RECORDS_PER_BUFFER);
      
      _parentIdsMapReader = 
          new DualBufferBinaryRecordReader(
              bfm.getIdMapFile(study, parentEntity, Operation.READ), 
              _parentIdsMapDeserializer.numBytes(), 
              RECORDS_PER_BUFFER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    // create writers
    final File idMapFile  = bfm.getIdMapFile(study, entity, Operation.WRITE).toFile();
    IdsMapConverter idsMapSerializer = new IdsMapConverter(entityAncestorsCount);
    _idsMapWriter = getIdsMapWriter(idMapFile, idsMapSerializer);
    
    final File ancestorsFile  = bfm.getAncestorFile(study, entity, Operation.WRITE).toFile();
    _ancestorsWriter = getAncestorsWriter(ancestorsFile, 
        new ListConverter<Long>(new LongValueConverter(), entityAncestorsCount + 1));
  }
  
  @Override
  public void consumeRow(List<String> row) throws IOException {
    if (_firstRow) { _firstRow = false; return; } // skip header
    
    Long curIdIndex = _idIndex.getAndIncrement();
    String curStringId = row.get(ID_COLUMN_INDEX);
    List<String> ancestorIds = row.subList(1, row.size());
    
    // write out ids_map file row
    IdsMap idsMap = new IdsMap(curIdIndex, curStringId, ancestorIds);
    _idsMapWriter.writeValue(idsMap);
        
   // write out ancestors file row
   advanceParentStreams(row.get(PARENT_ID_COLUMN_INDEX));
   List<Long> ancestorsRow = new ArrayList<Long>();
    ancestorsRow.add(curIdIndex);
    ancestorsRow.addAll(_currentParentAncestorRow);
    _ancestorsWriter.writeValue(ancestorsRow);
  }
  
  @Override
  public void close() throws Exception {
    _parentAncestorsReader.close();
    _parentIdsMapReader.close();
    _idsMapWriter.close();
    _ancestorsWriter.close();
  }
  
  /* advance parent streams to our current ID (if needed)
   *   - read parent id map file to compare parent string ID with what we got from the tabular stream
   *   - keep reading until they match
   *   - in parallel read parent's ancestor file
   *   - validate that the row from parent's id map file matches row from parent's ancestor file
   *   - globally remember parent id string and ancestor file row
   */
  private void advanceParentStreams(String parentIdString) {
    
    while (!parentIdString.equals(_currentParentIdString)) {
      
      // remember current parent ID string from parent ids_map file
      IdsMap parentIdsMapRow = 
          _parentIdsMapReader
          .next()
          .map(_parentIdsMapDeserializer::fromBytes)
          .orElseThrow(() -> new RuntimeException("Unexpected end of parent ids_map file"));
      _currentParentIdString = parentIdsMapRow.getEntityId();
      
      // remember current parent ancestor file row
      _currentParentAncestorRow = _parentAncestorsReader.next().map(_parentAncestorsDeserializer::fromBytes)
          .orElseThrow(() -> new RuntimeException("Unexpected end of parent ancestors file"));

      // validate, for the heck of it
      Long L = Long.valueOf(parentIdsMapRow.getIdIndex());
      if (L.equals(_currentParentAncestorRow.get(ID_COLUMN_INDEX)))
        throw new RuntimeException("Unexpected parent idIndex.  idMap: " + parentIdsMapRow.getIdIndex() + " ancestor: " + _currentParentAncestorRow.get(ID_COLUMN_INDEX));
    }   
  }

}