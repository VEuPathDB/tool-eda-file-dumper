package org.veupathdb.eda.binaryfiles.dumper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.eda.binaryfiles.BinaryFilesManager;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.eda.binaryfiles.BinaryFilesManager.Operation;
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
 *  
 * @author sfischer
 *
 */
public class IdFilesDumper implements FilesDumper {
  private static final int RECORDS_PER_BUFFER = 100;
  private final static int ID_COLUMN_INDEX = 0;  // the position in tabular stream of entity ID
  private final static int PARENT_ID_COLUMN_INDEX = 1; // the position in the tabular stream of the parent's ID

  private final ListConverter<Long> _parentAncestorConverter;
  private final ValueWithIdDeserializer<String> _parentIdMapDeserializer;
  private final Optional<DualBufferBinaryRecordReader> _parentAncestorReader;
  private final DualBufferBinaryRecordReader _parentIdMapReader;
  private final BinaryValueWriter<VariableValueIdPair<String>> _idMapWriter;
  private final BinaryValueWriter<List<Long>> _ancestorsWriter;
  boolean _firstRow = true;
  
  private List<Long> _currentParentAncestorRow;
  private String _currentParentIdString = "initialized to a non-existent ID";

  private AtomicLong _idIndex = new AtomicLong(0);

  public IdFilesDumper(BinaryFilesManager bfm, Study study, Entity entity, Entity parentEntity) {

    // create input readers
    _parentAncestorConverter = new ListConverter<>(new LongValueConverter(), entity.getAncestorEntities().size());
    _parentIdMapDeserializer = new ValueWithIdDeserializer<String>(new StringValueConverter(BYTES_RESERVED_FOR_ID_STRING));
    try {
      // only need parent ancestor reader if parent has ancestors
      _parentAncestorReader = parentEntity.getAncestorEntities().size() != 0?
          Optional.of(
          new DualBufferBinaryRecordReader(
              bfm.getAncestorFile(study, parentEntity, Operation.READ), 
              _parentAncestorConverter.numBytes(), 
              RECORDS_PER_BUFFER))
          : Optional.empty();
          
          // if no ancestor reader then initialize the parent ancestor row.
          if (_parentAncestorReader.isEmpty()) {
            _currentParentAncestorRow = new ArrayList<Long>();
            _currentParentAncestorRow.add(0L);  // initialize with a fake ID.  will be overwritten.
          }
      
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
   List<Long> ancestorsRow = new ArrayList<Long>(_currentParentAncestorRow);
    ancestorsRow.add(curIdIndex);
    _ancestorsWriter.writeValue(ancestorsRow);
  }
  
  @Override
  public void close() throws Exception {
    _parentAncestorReader.ifPresent(r -> r.close());
    _parentIdMapReader.close();
    _idMapWriter.close();
    _ancestorsWriter.close();
  }

  private BinaryValueWriter<List<Long>> getAncestorsWriter(File file, ListConverter<Long> converter) {
    try {
      final FileOutputStream outStream = new FileOutputStream(file);
      final BufferedOutputStream bufStream = new BufferedOutputStream(outStream);
      return new BinaryValueWriter<List<Long>>(bufStream, converter);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }    
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
      
      // remember current parent ID string
      VariableValueIdPair<String> parentIdMapRow = 
          _parentIdMapReader
          .next()
          .map(_parentIdMapDeserializer::fromBytes)
          .orElseThrow(() -> new RuntimeException("Unexpected end of parent id map file"));
      _currentParentIdString = parentIdMapRow.getValue();
      
      // remember current parent ancestor row
      // if parent does have ancestors, read its ancestor file
      // otherwise fake it by creating a row with just the parent's idIndex
      if (_parentAncestorReader.isPresent()) {
      _currentParentAncestorRow = _parentAncestorReader.get().next().map(_parentAncestorConverter::fromBytes)
          .orElseThrow(() -> new RuntimeException("Unexpected end of parent ancestors file"));
      } else {
        _currentParentAncestorRow.set(0, parentIdMapRow.getIdIndex());
      }

      // validate, for the heck of it
      if (_currentParentAncestorRow.size() != ID_COLUMN_INDEX)
        throw new RuntimeException("Unexpected parent ancestor row size: " + _currentParentAncestorRow.size());
      if (parentIdMapRow.getIdIndex() != _currentParentAncestorRow.get(ID_COLUMN_INDEX - 1))
        throw new RuntimeException("Unexpected parent idIndex");
    }   
  }

}
