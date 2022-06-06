package org.veupathdb.eda.dumper.io;

import java.io.File;
import java.nio.file.Path;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.Variable;

public class BinaryFilesManager {
  final Path _studiesDirectory;
  
  /* 
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

  public BinaryFilesManager(Path studiesDirectory) {
    _studiesDirectory = studiesDirectory;
  }
  
  /*
   * throw exception if already exists
   */
  void createStudyDir(Study study) {
  }
  
  /*
   * throw exception if does not exist
   */
  private Path getStudyDir(Study study) {
    return null;
  }
  
  /*
   * throw exception if already exists
   */
  void createEntityDir(Entity entity) {
  }
  
  /*
   * throw exception if does not exist
   */
  private Path getEntityDir(Entity entity) {
    return null;
  }

  File createAncestorFile(Entity entity) {
    return null;
  }
  
  File getAncestorFile(Entity entity) {
    return null;
  }
  
  File createIdFile(Entity entity) {
    return null;
  }

  File getIdFile(Entity entity) {
    return null;
  }
  
  File createVariableFile(Entity entity, Variable var) {
    return null;
  }

}
