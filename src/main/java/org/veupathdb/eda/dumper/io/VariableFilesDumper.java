package org.veupathdb.eda.dumper.io;

import java.io.IOException;
import java.util.List;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class VariableFilesDumper implements FilesDumper {

  public VariableFilesDumper(BinaryFilesManager bfm, Study study, Entity entity, VariableWithValues valueVar) {
    // TODO create and open relevant files for writing
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
    // TODO write rows to each file for each passed row; maybe extra for vocabs, etc.
  }

  @Override
  public void close() throws Exception {
    // TODO close files opened in constructor
  }

}
