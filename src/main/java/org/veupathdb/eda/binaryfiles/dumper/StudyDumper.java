package org.veupathdb.eda.binaryfiles.dumper;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Supplier;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.tabular.TabularHeaderFormat;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager.Operation;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.db.FilteredResultFactory;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class StudyDumper {
  private static final int INDEX_OF_ID = 0;
  private static final Logger LOG = LogManager.getLogger(StudyDumper.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final DataSource _dataSource;
  private final Study _study;
  private final Path _studiesDirectory;
  private final String _appDbSchema;
  private final BinaryFilesManager _bfm;

  public StudyDumper(DataSource dataSource, String appDbSchema, Path studiesDirectory, Study study) {
    _dataSource = dataSource;
    _appDbSchema = appDbSchema;
    _studiesDirectory = studiesDirectory;
    _study = study;
    _bfm = new BinaryFilesManager(_studiesDirectory);
  }
  
  public void dumpStudy() {
    _bfm.getStudyDir(_study, Operation.WRITE);
    TreeNode<Entity> entityTree = _study.getEntityTree();
    Entity rootEntity = entityTree.getContents();
    
    // Root study gets a special IDs file dumper (doesn't need parent ancestors)
    dumpSubtree(entityTree, new IdFilesDumperFactory(_bfm, _study, rootEntity, null), new HashMap<>());
    writeDoneFile(_bfm.getStudyDir(_study, Operation.READ), _study.getLastModified().getTime());
  }

  private void dumpSubtree(TreeNode<Entity> subTree, IdFilesDumperFactory idDumperFactory, Map<String, Integer> entityIdToMaxIdLength) {
    Entity entity = subTree.getContents();

    // Find the maximum length of an ID for this entity to determine space to allocate for each ID.
    // Add 4 to store the size of the ID as well.
    int maxIdLength = scanForMaxLength(entity);

    // Add the value to the Map so that it is available when dumping subtrees recursively.
    entityIdToMaxIdLength.put(entity.getId(), maxIdLength);

    dumpEntity(entity, idDumperFactory, entityIdToMaxIdLength);

    for (TreeNode<Entity> child : subTree.getChildNodes()) {
      dumpSubtree(child,
          new IdFilesDumperFactory(_bfm, _study, child.getContents(), entity),
          entityIdToMaxIdLength);
    }
  }
  
  private void dumpEntity(Entity entity, IdFilesDumperFactory idDumperFactory, Map<String, Integer> entityIdToMaxIdLength) {
    _bfm.getEntityDir(_study, entity, Operation.WRITE);

    // create an object that will track minor meta info about the files, sufficient to parse them into text
    BinaryFilesManager.Metadata metadata = new BinaryFilesManager.Metadata();

    List<Integer> bytesReservedPerAncestor = new ArrayList<>();
    // Order is important, should be written in the same order as the ancestorEntities array list.
    for (Entity ancestor: entity.getAncestorEntities()) {
      final String ancestorEntityId = ancestor.getId();
      bytesReservedPerAncestor.add(entityIdToMaxIdLength.get(ancestorEntityId));
    }
    metadata.setBytesReservedPerAncestor(bytesReservedPerAncestor);
    metadata.setBytesReservedForId(entityIdToMaxIdLength.get(entity.getId()));
    // first select no variables to dump the ID and ancestors files
    handleResult(_dataSource, _study, entity, Optional.empty(), () -> idDumperFactory.create(entityIdToMaxIdLength));

    List<BinaryFilesManager.VariableMeta> variableMetadata = new ArrayList<>();
    // loop through variables, creating a file for each
    for (Variable variable : entity.getVariables()) {
      
      if (!variable.hasValues()) continue; // skip categories
      VariableWithValues<?> valueVar = (VariableWithValues<?>)variable;

      BinaryFilesManager.VariableMeta varMeta = new BinaryFilesManager.VariableMeta();
      varMeta.setVariableId(variable.getId());
      varMeta.setType(valueVar.getType().getTypeString());
      varMeta.setProperties(valueVar.getBinaryProperties());
      variableMetadata.add(varMeta);

      handleResult(_dataSource, _study, entity, Optional.of(valueVar), () -> new VariableFilesDumper<>(_bfm, _study, entity, valueVar));
      handleResult(_dataSource, _study, entity, Optional.of(valueVar), () -> new VariableFilesStringDumper<>(_bfm, _study, entity, valueVar));
    }
    metadata.setVariableMetadata(variableMetadata);
    
    writeMetaJsonFile(metadata, entity);
    writeDoneFile(_bfm.getEntityDir(_study, entity, Operation.READ), _study.getLastModified().getTime());
  }

  private void handleResult(DataSource ds, Study study, Entity entity, Optional<VariableWithValues> variable, Supplier<FilesDumper> dumperSupplier) {
    List<VariableWithValues> vars = variable
        .map(List::of)
        .orElse(Collections.emptyList());
    try (FilesDumper dumper = dumperSupplier.get()) {
      FilteredResultFactory.produceTabularSubset(ds, _appDbSchema, study, entity, vars, List.of(), new TabularReportConfig(), dumper);
    }
    catch (Exception e) {
      LOG.warn("Failed to dump files for study: {}, entity: {}, variable: {}", study.getStudyId(), entity.getId(),
          variable.map(var -> var.getId()).orElse("none"), e);
      // TODO Remove this exception swallowing once long free-text variables are accounted for.
    }
  }


  private int scanForMaxLength(Entity entity) {
    return scanForMaxLength(entity, null);
  }

  private int scanForMaxLength(Entity entity, VariableWithValues variable) {
    List<VariableWithValues> variables = variable == null ? List.of() : List.of(variable);
    try {
      final TabularReportConfig reportConfig = new TabularReportConfig();
      reportConfig.setHeaderFormat(TabularHeaderFormat.STANDARD);
      int index = variable == null ? INDEX_OF_ID : entity.getAncestorEntities().size() + 1;
      final MaxLengthFinder maxLengthFinder = new MaxLengthFinder(index);
      FilteredResultFactory.produceTabularSubset(_dataSource, _appDbSchema, _study, entity, variables, List.of(), new TabularReportConfig(), maxLengthFinder);
      return maxLengthFinder.getMaxLength();
    }
    catch (Exception e) {
      throw new RuntimeException("Could not dump files for study " + _study.getStudyId(), e);
    }
  }


  private void writeMetaJsonFile(BinaryFilesManager.Metadata metadata, Entity entity) {
    try (FileWriter writer = new FileWriter(_bfm.getMetaJsonFile(_study, entity, Operation.WRITE).toFile())) {
      OBJECT_MAPPER.writeValue(writer, metadata);
    } catch (IOException e) {
      throw new RuntimeException("Failed writing meta.json file.", e);
    }   
  }
  
  private void writeDoneFile(Path directory, long version) {
    try (FileWriter writer = 
        new FileWriter(_bfm.getDoneFile(directory, Operation.WRITE).toFile())) {
      Map<String, Long> filesMeta = Map.of("dataVersion", version);
      OBJECT_MAPPER.writeValue(writer, filesMeta);
    } catch (IOException e) {
      throw new RuntimeException("Failed writing DONE file.", e);
    }   
  }
  
}
