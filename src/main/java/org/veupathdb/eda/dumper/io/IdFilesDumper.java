package org.veupathdb.eda.dumper.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.converter.BinarySerializer;
import org.veupathdb.service.eda.ss.model.variable.converter.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.TupleSerializer;

public class IdFilesDumper implements FilesDumper {
  private static final int RECORDS_PER_BUFFER = 100;

  private DualBufferBinaryRecordReader ancestorReader;
  private final BinarySerializer<List<Long>> serializer;

  public IdFilesDumper(BinaryFilesManager bfm, Study study, Entity entity, Entity parentEntity) {
    final Path ancestorFilePath = bfm.getAncestorFile(study, entity);
    final int ancestorCount = entity.getAncestorEntities().size();
    this.serializer = new TupleSerializer<>(new LongValueConverter(), ancestorCount);
    try {
      this.ancestorReader = new DualBufferBinaryRecordReader(ancestorFilePath, serializer.numBytes(), RECORDS_PER_BUFFER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // TODO create and open relevant files for writing
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
    Optional<List<Long>> ancestorRow = ancestorReader.next().map(serializer::fromBytes);
    // TODO write rows to each file for each passed row
  }

  @Override
  public void close() throws Exception {
    this.ancestorReader.close();
  }

}
