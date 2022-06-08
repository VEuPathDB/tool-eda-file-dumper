package org.veupathdb.eda.dumper.io;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.converter.StringValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdSerializer;

/**
 * Reads from:
 *  - tabular stream
 * Writes to:
 *  - idMap
 *   
 * @author sfischer
 *
 */
public class IdFilesDumperForRoot implements FilesDumper {
  private static final int BYTES_RESERVED_FOR_ID = 30;
  private BinaryValueWriter<VariableValueIdPair<String>> binaryValueWriter;
  private AtomicLong index = new AtomicLong(0);

  public IdFilesDumperForRoot(BinaryFilesManager bfm, Study study, Entity entity) {
    // TODO create and open relevant files for writing
    Path idMappingFile = bfm.getIdMapFile(study, entity);
    final File file = idMappingFile.toFile();
    try {
      final FileOutputStream fileOutputStream = new FileOutputStream(file);
      final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      final ValueConverter<String> stringConverter = new StringValueConverter(BYTES_RESERVED_FOR_ID);
      final ValueWithIdSerializer<String> serializer = new ValueWithIdSerializer<>(stringConverter);
      this.binaryValueWriter = new BinaryValueWriter<>(bufferedOutputStream, serializer);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
    VariableValueIdPair<String> record = new VariableValueIdPair<>(index.getAndIncrement(), extractIdFromRow(row));
    this.binaryValueWriter.writeValue(record);
    // TODO write rows to each file for each passed row
  }

  @Override
  public void close() throws Exception {
    this.binaryValueWriter.close();
  }

  private String extractIdFromRow(List<String> row) {
    // TODO fill this in or move it in-line to consumeRow.
    return null;
  }
}
