package org.veupathdb.eda.binaryfiles.dumper;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.gusdb.fgputil.functional.TreeNode;
import org.json.JSONArray;
import org.json.JSONObject;
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
    writeDoneFile(_bfm.getStudyDir(_study, Operation.READ));
  }

  private void dumpSubtree(TreeNode<Entity> subTree, IdFilesDumperFactory idDumperFactory, Map<String, Integer> entityIdToMaxIdLength) {
    Entity entity = subTree.getContents();

    // Find the maximum length of an ID for this entity to determine space to allocate for each ID.
    // Add 4 to store the size of the ID as well.
    int maxIdLength = scanForMaxIdLength(entity);

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
    JSONObject metaJson = new JSONObject();

    JSONArray ancestorsJson = new JSONArray();
    // Order is important, should be written in the same order as the ancestorEntities array list.
    for (Entity ancestor: entity.getAncestorEntities()) {
      String ancestorEntityId = ancestor.getId();
      ancestorsJson.put(entityIdToMaxIdLength.get(ancestorEntityId));
    }

    metaJson.put(BinaryFilesManager.META_KEY_BYTES_PER_ANCESTOR, ancestorsJson);
    metaJson.put(BinaryFilesManager.META_KEY_BYTES_FOR_ID, entityIdToMaxIdLength.get(entity.getId()));

    // first select no variables to dump the ID and ancestors files
    handleResult(_dataSource, _study, entity, Optional.empty(), () -> idDumperFactory.create(entityIdToMaxIdLength));

    // loop through variables, creating a file for each
    for (Variable variable : entity.getVariables()) {
      
      if (!variable.hasValues()) continue; // skip categories
      VariableWithValues<?> valueVar = (VariableWithValues<?>)variable;
      
      handleResult(_dataSource, _study, entity, Optional.of(valueVar), () -> new VariableFilesDumper<>(_bfm, _study, entity, valueVar));

      metaJson.put(_bfm.getVariableFile(_study, entity, variable, Operation.READ).getFileName().toString(), 
                   valueVar.getType().getTypeString()); 
    }   
    
    writeMetaJsonFile(metaJson, entity);
    writeDoneFile(_bfm.getEntityDir(_study, entity, Operation.READ));
  }

  private void handleResult(DataSource ds, Study study, Entity entity, Optional<VariableWithValues> variable, Supplier<FilesDumper> dumperSupplier) {
    List<VariableWithValues> vars = variable.map(List::of).orElse(Collections.emptyList());
    try (FilesDumper dumper = dumperSupplier.get()) {
      FilteredResultFactory.produceTabularSubset(ds, _appDbSchema, study, entity, vars, List.of(), new TabularReportConfig(), dumper);
    }
    catch (Exception e) {
      throw new RuntimeException("Could not dump files for study " + study.getStudyId(), e);
    }
  }

  private int scanForMaxIdLength(Entity entity) {
    try {
      final TabularReportConfig reportConfig = new TabularReportConfig();
      reportConfig.setHeaderFormat(TabularHeaderFormat.STANDARD);
      final MaxIdLengthFinder maxIdLengthFinder = new MaxIdLengthFinder();
      FilteredResultFactory.produceTabularSubset(_dataSource, _appDbSchema, _study, entity, List.of(), List.of(), new TabularReportConfig(), maxIdLengthFinder);
      return maxIdLengthFinder.getMaxLength();
    }
    catch (Exception e) {
      throw new RuntimeException("Could not dump files for study " + _study.getStudyId(), e);
    }
  }


  private void writeMetaJsonFile(JSONObject metaJson, Entity entity) {
    try (FileWriter writer = new FileWriter(_bfm.getMetaJsonFile(_study, entity, Operation.WRITE).toFile())) {
      writer.write(metaJson.toString(2));
      writer.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed writing meta.json file.", e);
    }   
  }
  
  private void writeDoneFile(Path directory) {
    try (FileWriter writer = 
        new FileWriter(_bfm.getDoneFile(directory, Operation.WRITE).toFile())) {
      writer.write("");
      writer.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed writing DONE file.", e);
    }   
  }

  
}
