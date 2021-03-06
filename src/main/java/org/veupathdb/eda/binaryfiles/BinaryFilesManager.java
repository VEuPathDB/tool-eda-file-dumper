package org.veupathdb.eda.binaryfiles;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.Variable;

public class BinaryFilesManager {
  final Path _studiesDirectory;
  
  public static final String ANCESTORS_FILE_NAME = "ancestors";
  public static final String IDS_MAP_FILE_NAME = "ids_map";
  private static final String STUDY_FILE_PREFIX = "study_";
  private static final String ENTITY_FILE_PREFIX = "entity_";
  private static final String VAR_FILE_PREFIX = "var_";
  private static final String VOCAB_FILE_PREFIX = "vocab_";
  
  public static final String META_KEY_NUM_ANCESTORS = "numAncestors";
  static final String DONE_FILE_NAME = "DONE";
  
  public enum Operation { READ, WRITE };

  private static final Logger LOG = LogManager.getLogger(BinaryFilesManager.class);

  /* 
 studies/
  study_GEMS1A/
    DONE                 # empty file indicating study dump completed successfully   
    entity_EUPA_12345/   # an entity ID
      ancestors          # ancestor ID indices.
      ids_map            # index -> ID map
      meta.json          # simple info about the files
      var_EUPA_44444     # a variable file
      var_EUPA_55555     # another variable file
      vocab_EUPA_55555   # a vocabulary file
   *     
   */

  public BinaryFilesManager(Path studiesDirectory) {
    _studiesDirectory = studiesDirectory;
  }
  
  public Path getStudyDir(Study study, Operation op) {
    if (op == Operation.READ) return getStudyDir(study);
    else {
      Path studyDir = Path.of(_studiesDirectory.toString(), getStudyDirName(study));
      createDir(studyDir);
      return studyDir;
    }
  }

  public Path getEntityDir(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getEntityDir(study, entity);
    else {
      Path entityDir = Path.of(_studiesDirectory.toString(), getStudyDir(study).getFileName().toString(), getEntityDirName(entity));
      createDir(entityDir);
      return entityDir;
    }
  }
  
  public Path getAncestorFile(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, ANCESTORS_FILE_NAME);
    return createFile(study, entity, ANCESTORS_FILE_NAME);
  }

  public Path getIdMapFile(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, IDS_MAP_FILE_NAME);
    return createFile(study, entity, IDS_MAP_FILE_NAME);
  }
  
  public Path getVariableFile(Study study, Entity entity, Variable var, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, getVarFileName(var));
    return createFile(study, entity, getVarFileName(var));
  }
  
  Path getVocabFile(Study study, Entity entity, Variable var, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, getVocabFileName(var));
    return createFile(study, entity, getVocabFileName(var));
  }
  
  public Path getMetaJsonFile(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, "meta.json");   
    return createFile(study, entity, "meta.json");   
  }
 
  public Path getDoneFile(Path directory, Operation op) {
    if (op == Operation.WRITE) return createDoneFile(directory);
    
    Path filepath = Path.of(directory.toString(), DONE_FILE_NAME);
    if (!Files.exists(filepath)) throw new RuntimeException("File '" + filepath + "' does not exist");
    return filepath;
  }
  
  ////////////////////////////////////////////////////////
  
  private String getStudyDirName(Study study) {
    return STUDY_FILE_PREFIX + study.getInternalAbbrev();
  }
  
  private String getEntityDirName(Entity entity) {
    return ENTITY_FILE_PREFIX + entity.getId();
  }
  
  private String getVarFileName(Variable var) {
    return VAR_FILE_PREFIX + var.getId();
  }
  
  private String getVocabFileName(Variable var) {
    return VOCAB_FILE_PREFIX + var.getId();
  }
  
  private Path createFile(Study study, Entity entity, String filename) {

    Path entityDir = getEntityDir(study, entity);
    Path filepath = Path.of(entityDir.toString(), filename);
    LOG.info("Creating file: " + filepath);
    try {
      Files.createFile(filepath);
    } catch (FileAlreadyExistsException e) {
      throw new RuntimeException("Failed creating file '" + filepath + "'.  It already exists.", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }  
    return filepath;
  }
  
  private Path getFile(Study study, Entity entity, String fileName) {
    Path entityDir = getEntityDir(study, entity);
    Path file = Path.of(entityDir.toString(), fileName);
    if (!Files.exists(file)) throw new RuntimeException("File '" + file + "' does not exist");
    return file;
  }
  
  private void createDir(Path dir) {
    LOG.info("Creating dir: " + dir);
    try {
      Files.createDirectory(dir);
    } catch (FileAlreadyExistsException e) {
      throw new RuntimeException("Failed creating directory '" + dir + "'.  It already exists.", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }          
  }
  
  private Path getEntityDir(Study study, Entity entity) {
    Path entityDir = Path.of(_studiesDirectory.toString(), getStudyDirName(study), getEntityDirName(entity));
    if (!Files.isDirectory(entityDir)) throw new RuntimeException("Entity directory '" + entityDir + "' does not exist");
    return entityDir;
  }

  private Path getStudyDir(Study study) {
    Path studyDir = Path.of(_studiesDirectory.toString(), getStudyDirName(study));
    if (!Files.isDirectory(studyDir)) throw new RuntimeException("Study directory '" + studyDir + "' does not exist");
    return studyDir;
  }

  private Path createDoneFile(Path directory) {
    Path filepath = Path.of(directory.toString(), DONE_FILE_NAME);
    LOG.info("Creating file: " + filepath);
    try {
      Files.createFile(filepath);
    } catch (FileAlreadyExistsException e) {
      throw new RuntimeException("Failed creating file '" + filepath + "'.  It already exists.", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }  
    return filepath;
  }
  

  
}
