package org.veupathdb.eda.dumper;

import static org.gusdb.fgputil.runtime.Environment.getRequiredVar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;
import org.veupathdb.eda.dumper.io.FilesDumper;
import org.veupathdb.eda.dumper.io.IdFilesDumper;
import org.veupathdb.eda.dumper.io.VariableFilesDumper;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.db.FilteredResultFactory;
import org.veupathdb.service.eda.ss.model.db.StudyFactory;
import org.veupathdb.service.eda.ss.model.db.StudyResolver;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class Main {

  private static final String APP_DB_SCHEMA = "eda.";

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("USAGE: dumpFiles <studyId> <parentDirectory>");
      System.exit(1);
    }

    String studyId = args[0];
    Path parentDirectory = Paths.get(args[1]);

    if (!Files.isDirectory(parentDirectory) || !Files.isWritable(parentDirectory)) {
      throw new IllegalArgumentException(parentDirectory.toAbsolutePath() + " is not a writable directory.");
    }

    // read required environment vars
    String connectionUrl = getRequiredVar("DB_CONNECTION_URL");
    String connectionUser = getRequiredVar("DB_USERNAME");
    String connectionPassword = getRequiredVar("DB_PASSWORD");

    // instantiate a connection to the database
    try (DatabaseInstance appDb = new DatabaseInstance(SimpleDbConfig.create(
        SupportedPlatform.ORACLE, connectionUrl, connectionUser, connectionPassword))) {

      DataSource ds = appDb.getDataSource();
      StudyFactory studyFactory = new StudyFactory(ds, APP_DB_SCHEMA, false,false);
      Study study = studyFactory.getStudyById(studyId);

      for (Entity entity : study.getEntityTree().flatten()) {

        // first select no variables to dump the ID and ancestors files
        handleResult(ds, study, entity, Optional.empty(), () -> new IdFilesDumper(parentDirectory, study, entity));

        // loop through variables, creating a file for each
        for (Variable variable : entity.getVariables()) {
          if (!variable.hasValues()) continue; // skip categories
          VariableWithValues<?> valueVar = (VariableWithValues<?>)variable;
          handleResult(ds, study, entity, Optional.of(valueVar), () -> new VariableFilesDumper(parentDirectory, study, entity, valueVar));
        }
      }
    }
  }

  private static void handleResult(DataSource ds, Study study, Entity entity, Optional<Variable> variable, Supplier<FilesDumper> dumperSupplier) {
    List<Variable> vars = variable.map(List::of).orElse(Collections.emptyList());
    try (FilesDumper dumper = dumperSupplier.get()) {
      FilteredResultFactory.produceTabularSubset(ds, APP_DB_SCHEMA, study, entity, vars, List.of(), new TabularReportConfig(), dumper);
    }
    catch (Exception e) {
      throw new RuntimeException("Could not dump files for study " + study.getStudyId());
    }
  }
}
