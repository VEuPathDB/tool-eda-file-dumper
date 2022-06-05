package org.veupathdb.eda.dumper.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;

public class IdFilesDumper implements FilesDumper {
  
  /* thoughts
   *  1. we need a class to represent this entity's persistent structure
   *     - List<RowList> getAncestorRowLists()
   *     - File getIDFile();
   *     
 studies/
  GEMS1A/
    EUPA_12345/      # an entity ID
      ancestors_ix   # ancestor ID indices.
      ids_ix         # index -> ID map
      EUPA_44444     # a variable file
      EUPA_55555     # another variable file
      EUPA_55555_v   # a vocabulary file
   *     
   */
  
  private final Optional<File> _parentAncestorFile;

  public IdFilesDumper(Path parentDirectory, Study study, Entity entity, Optional<File> parentAncestorFile) {
    // TODO create and open relevant files for writing
    _parentAncestorFile = parentAncestorFile;
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
