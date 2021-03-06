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
    String connectionUrl = getRequiredVar("APP_DB_CONNECT");
    String connectionUser = getRequiredVar("APP_DB_USER");
    String connectionPassword = getRequiredVar("APP_DB_PASS");

    // instantiate a connection to the database
    try (DatabaseInstance appDb = new DatabaseInstance(SimpleDbConfig.create(
        SupportedPlatform.ORACLE, connectionUrl, connectionUser, connectionPassword))) {

      DataSource ds = appDb.getDataSource();
      StudyFactory studyFactory = new StudyFactory(ds, APP_DB_SCHEMA, StudyOverview.StudySourceType.CURATED, false);
      Study study = studyFactory.getStudyById(studyId);
      
      StudyDumper studyDumper = new StudyDumper(ds, APP_DB_SCHEMA, studiesDirectory, study);
      studyDumper.dumpStudy();

    }
  }
  
}
