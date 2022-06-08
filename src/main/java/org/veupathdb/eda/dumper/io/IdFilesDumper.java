package org.veupathdb.eda.dumper.io;

import java.io.IOException;
import java.util.List;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;

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
 
  public IdFilesDumper(BinaryFilesManager bfm, Study study, Entity entity, Entity parentEntity) {
    // TODO create and open relevant files for writing
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
    // TODO write rows to each file for each passed row
  }

  @Override
  public void close() throws Exception {
    // TODO close files opened in constructor
  }

}
