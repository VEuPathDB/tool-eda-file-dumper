package org.veupathdb.eda.binaryfiles.dumper;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.veupathdb.service.eda.ss.model.variable.binary.StudyFinder;

import javax.sql.DataSource;
import java.nio.file.Path;

public class ProjectSpecificStudyFinder implements StudyFinder {
  private DataSource dataSource;
  private String root;

  public ProjectSpecificStudyFinder(String root, DataSource dataSource) {
    this.dataSource = dataSource;
    this.root = root;
  }

  @Override
  public Path findStudyPath(String studyDirName) {
    final String sql = generateSql(studyDirName.substring("study_".length()));
    String project = new SQLRunner(dataSource, sql).executeQuery(rs -> rs.getString(0));
    return Path.of(root, project, studyDirName);
  }

  private String generateSql(String studyId) {
    return "SELECT name FROM core.projectinfo projects JOIN study.study study ON study.row_project_id = projects.project_id WHERE study.study_id = " + studyId;
  }
}
