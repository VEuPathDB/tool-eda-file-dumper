package org.veupathdb.eda.binaryfiles.dumper;

import static org.gusdb.fgputil.runtime.Environment.getRequiredVar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.sql.DataSource;

import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudyOverview;
import org.veupathdb.service.eda.ss.model.db.StudyFactory;
import org.veupathdb.service.eda.ss.model.db.VariableFactory;
import org.veupathdb.service.eda.ss.model.reducer.EmptyBinaryMetadataProvider;


public class Main {

  private static final String APP_DB_SCHEMA = "eda.";

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("USAGE: dumpFiles <studyId> <parentDirectory>");
      System.exit(1);
    }

    String studyId = args[0];
    Path studiesDirectory = Paths.get(args[1]);

    if (!Files.isDirectory(studiesDirectory) || !Files.isWritable(studiesDirectory)) {
      throw new IllegalArgumentException(studiesDirectory.toAbsolutePath() + " is not a writable directory.");
    }

    // read required environment vars
    String connectionUrl = getRequiredVar("APPDB_CONNECT");
    String connectionUser = getRequiredVar("APPDB_USER");
    String connectionPassword = getRequiredVar("APPDB_PASS");

    // instantiate a connection to the database
    try (DatabaseInstance dbInstance = new DatabaseInstance(SimpleDbConfig.create(
         SupportedPlatform.ORACLE, connectionUrl, connectionUser, connectionPassword, 2))) {
      DataSource dataSource = dbInstance.getDataSource();

      // Create a variable factory which can provide undecorated study metadata. This is used to provide the necessary
      // metadata to generate binary files.
      VariableFactory undecoratedVariableFactory = new VariableFactory(dataSource, APP_DB_SCHEMA, new EmptyBinaryMetadataProvider());
      StudyFactory undecoratedStudyFactory = new StudyFactory(dataSource, APP_DB_SCHEMA, StudyOverview.StudySourceType.CURATED, undecoratedVariableFactory);
      Study undecoratedStudy = undecoratedStudyFactory.getStudyById(studyId);
      ScanningBinaryMetadataProvider metadataProvider = new ScanningBinaryMetadataProvider(undecoratedStudy, dataSource, APP_DB_SCHEMA);

      // Create variable and study factories used to provide binary encoding metadata.
      VariableFactory metadataDecoratedStudyFactory = new VariableFactory(dataSource, APP_DB_SCHEMA, metadataProvider);
      StudyFactory studyFactory = new StudyFactory(dataSource, APP_DB_SCHEMA, StudyOverview.StudySourceType.CURATED, metadataDecoratedStudyFactory);
      Study study = studyFactory.getStudyById(studyId);
      
      StudyDumper studyDumper = new StudyDumper(dataSource, APP_DB_SCHEMA, studiesDirectory, study);
      studyDumper.dumpStudy();
    }
  }
}
