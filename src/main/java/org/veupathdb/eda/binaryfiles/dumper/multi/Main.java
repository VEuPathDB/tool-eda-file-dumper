package org.veupathdb.eda.binaryfiles.dumper.multi;

import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;
import org.veupathdb.eda.binaryfiles.dumper.ScanningBinaryMetadataProvider;
import org.veupathdb.eda.binaryfiles.dumper.StudyDumper;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudyOverview;
import org.veupathdb.service.eda.ss.model.db.StudyFactory;
import org.veupathdb.service.eda.ss.model.db.StudyProvider;
import org.veupathdb.service.eda.ss.model.db.StudyResolver;
import org.veupathdb.service.eda.ss.model.db.VariableFactory;
import org.veupathdb.service.eda.ss.model.reducer.EmptyBinaryMetadataProvider;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;

import javax.sql.DataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.gusdb.fgputil.runtime.Environment.getRequiredVar;

public class Main {
  private static final String APP_DB_SCHEMA = "eda.";

  public static void main(String[] args) throws Exception {
    // read required environment vars
    String connectionUrl = getRequiredVar("APPDB_CONNECT");
    String connectionUser = getRequiredVar("APPDB_USER");
    String connectionPassword = getRequiredVar("APPDB_PASS");

    if (args.length != 1) {
      System.err.println("USAGE: dumpAllStudiesFiles <parentDirectory>");
      System.exit(1);
    }

    Path studiesDirectory = Paths.get(args[0]);

    if (!Files.isDirectory(studiesDirectory) || !Files.isWritable(studiesDirectory)) {
      throw new IllegalArgumentException(studiesDirectory.toAbsolutePath() + " is not a writable directory.");
    }

    List<FailedStudy> failedStudies = new ArrayList<>();
    final BinaryFilesManager binaryFilesManager = new BinaryFilesManager(studiesDirectory);

    // instantiate a connection to the database
    try (DatabaseInstance appDb = new DatabaseInstance(SimpleDbConfig.create(
        SupportedPlatform.ORACLE, connectionUrl, connectionUser, connectionPassword, 2))) {

      DataSource ds = appDb.getDataSource();
      VariableFactory undecoratedVarFactory = new VariableFactory(ds, APP_DB_SCHEMA, new EmptyBinaryMetadataProvider(), binaryFilesManager);
      StudyFactory undecoratedStudyFactory = new StudyFactory(ds, APP_DB_SCHEMA, StudyOverview.StudySourceType.CURATED, undecoratedVarFactory);

      for (StudyOverview studyOverview: undecoratedStudyFactory.getStudyOverviews()) {
        Study undecoratedStudy = undecoratedStudyFactory.getStudyById(studyOverview.getStudyId());
        ScanningBinaryMetadataProvider metadataProvider = new ScanningBinaryMetadataProvider(undecoratedStudy, ds, APP_DB_SCHEMA);
        VariableFactory variableFactory = new VariableFactory(ds, APP_DB_SCHEMA, metadataProvider, binaryFilesManager);
        StudyFactory studyFactory = new StudyFactory(ds, APP_DB_SCHEMA, StudyOverview.StudySourceType.CURATED, variableFactory);

        try {
          Study study = studyFactory.getStudyById(studyOverview.getStudyId());
          StudyDumper studyDumper = new StudyDumper(ds, APP_DB_SCHEMA, studiesDirectory, study);
          studyDumper.dumpStudy();
        } catch (Exception e) {
          e.printStackTrace();
          failedStudies.add(new FailedStudy(e, studyOverview));
        }
      }
    }
    if (!failedStudies.isEmpty()) {
      final String commaSeparatedStudies = failedStudies.stream()
          .map(study -> study.studyOverview.getStudyId())
          .collect(Collectors.joining(","));
      throw new RuntimeException("The following studies failed to be written: " + commaSeparatedStudies
          + ". Attaching first failure stack trace: ", failedStudies.stream().findFirst().get().e);
    }
  }

  public static class FailedStudy {
    private Exception e;
    private StudyOverview studyOverview;

    public FailedStudy(Exception e, StudyOverview studyOverview) {
      this.e = e;
      this.studyOverview = studyOverview;
    }
  }
}
