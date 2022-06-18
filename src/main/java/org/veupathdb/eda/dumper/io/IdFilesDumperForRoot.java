package org.veupathdb.eda.dumper.io;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
public class IdFilesDumperForRoot implements FilesDumper {
  private BinaryValueWriter<VariableValueIdPair<String>> _imfWriter;
  private AtomicLong _idIndex = new AtomicLong(0);

  public IdFilesDumperForRoot(BinaryFilesManager bfm, Study study, Entity entity) {
    final File imf  = bfm.getIdMapFile(study, entity).toFile();
    _imfWriter = getVarAndIdBinaryWriter(imf, new StringValueConverter(BYTES_RESERVED_FOR_ID_STRING));
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
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
