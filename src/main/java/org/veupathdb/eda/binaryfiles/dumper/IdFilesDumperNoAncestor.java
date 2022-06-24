package org.veupathdb.eda.binaryfiles.dumper;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.veupathdb.eda.binaryfiles.BinaryFilesManager;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.eda.binaryfiles.BinaryFilesManager.Operation;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

/**
 * Reads from:
 *  - tabular stream (strings)
 * Writes to:
 *  - idMap
 *   
 * @author sfischer
 *
 */
public class IdFilesDumperNoAncestor implements FilesDumper {
  private BinaryValueWriter<VariableValueIdPair<String>> _imfWriter;
  private AtomicLong _idIndex = new AtomicLong(0);
  boolean _firstRow = true;

  public IdFilesDumperNoAncestor(BinaryFilesManager bfm, Study study, Entity entity) {
    final File imf  = bfm.getIdMapFile(study, entity, Operation.WRITE).toFile();
    _imfWriter = getVarAndIdBinaryWriter(imf, new StringValueConverter(BYTES_RESERVED_FOR_ID_STRING));
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
    if (_firstRow) { _firstRow = false; return; } // skip header
    
    VariableValueIdPair<String> idMap = new VariableValueIdPair<>(_idIndex.getAndIncrement(), extractIdFromRow(row));
    _imfWriter.writeValue(idMap);
  }

  @Override
  public void close() throws Exception {
    _imfWriter.close();
  }

  private String extractIdFromRow(List<String> row) {
    if (row.size() != 1) throw new RuntimeException("Expected 1 column, got: " + row.size());
    return row.get(0);
  }
}
