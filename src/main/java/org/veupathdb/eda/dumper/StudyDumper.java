package org.veupathdb.eda.dumper;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.gusdb.fgputil.functional.TreeNode;
import org.json.JSONObject;
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
    _bfm.createStudyDir(_study);
    TreeNode<Entity> entityTree = _study.getEntityTree();
    Entity rootEntity = entityTree.getContents();
    
    // Root study gets a special IDs file dumper (doesn't need parent ancestors)
    dumpSubtree(entityTree, () -> new IdFilesDumperForRoot(_bfm, _study, rootEntity));
    writeDoneFile();
  }

  private void dumpSubtree(TreeNode<Entity> subTree, Supplier<FilesDumper> idsDumperSupplier) {
    Entity entity = subTree.getContents();
    
    dumpEntity(entity, idsDumperSupplier);
    
    for (TreeNode<Entity> child : subTree.getChildNodes()) {
      Entity childEntity = child.getContents();
      dumpSubtree(child, () -> new IdFilesDumper(_bfm, _study, childEntity, entity));
    }
  }
  
  private void dumpEntity(Entity entity, Supplier<FilesDumper> idsDumperSupplier) {
    
    // create an object that will track minor meta info about the files, sufficient to parse them into text
    JSONObject metaJson = new JSONObject();
    metaJson.put(BinaryFilesManager.META_KEY_NUM_ANCESTORS, entity.getAncestorEntities().size());
    
    // first select no variables to dump the ID and ancestors files
    handleResult(_dataSource, _study, entity, Optional.empty(), idsDumperSupplier);

    // loop through variables, creating a file for each
    for (Variable variable : entity.getVariables()) {
      
      if (!variable.hasValues()) continue; // skip categories
      VariableWithValues<?> valueVar = (VariableWithValues<?>)variable;
      
      handleResult(_dataSource, _study, entity, Optional.of(valueVar), () -> new VariableFilesDumper<>(_bfm, _study, entity, valueVar));

      metaJson.put(_bfm.getVariableFile(_study, entity, variable).getFileName().toString(), 
          valueVar.getType().name());
    }   
    
    writeMetaJsonFile(metaJson, entity);
  }

  private void handleResult(DataSource ds, Study study, Entity entity, Optional<Variable> variable, Supplier<FilesDumper> dumperSupplier) {
    List<Variable> vars = variable.map(List::of).orElse(Collections.emptyList());
    try (FilesDumper dumper = dumperSupplier.get()) {
      FilteredResultFactory.produceTabularSubset(ds, _appDbSchema, study, entity, vars, List.of(), new TabularReportConfig(), dumper);
    }
    catch (Exception e) {
      throw new RuntimeException("Could not dump files for study " + study.getStudyId());
    }
  }
  
  private void writeMetaJsonFile(JSONObject metaJson, Entity entity) {
    try (FileWriter writer = new FileWriter(_bfm.createMetaJsonFile(_study, entity).toFile())) {
      writer.write(metaJson.toString(2));
      writer.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed writing meta.json file.", e);
    }   
  }
  
  private void writeDoneFile() {
    try (FileWriter writer = new FileWriter(_bfm.createDoneFile(_study).toFile())) {
      writer.write("");
      writer.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed writing DONE file.", e);
    }   
  }

  
}
