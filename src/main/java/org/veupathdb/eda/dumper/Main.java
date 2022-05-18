package org.veupathdb.eda.dumper;

import static org.gusdb.fgputil.runtime.Environment.getRequiredVar;

import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;
import org.gusdb.fgputil.db.runner.SQLRunner;

public class Main {

  public static void main(String args[]) throws Exception {

    // read required environment vars
    String connectionUrl = getRequiredVar("DB_CONNECTION_URL");
    String connectionUser = getRequiredVar("DB_USERNAME");
    String connectionPassword = getRequiredVar("DB_PASSWORD");

    // instantiate a connection to the database
    try (DatabaseInstance db = new DatabaseInstance(SimpleDbConfig.create(
        SupportedPlatform.ORACLE, connectionUrl, connectionUser, connectionPassword))) {

      // do some db work here
      new SQLRunner(db.getDataSource(), "select 1 from dual").executeQuery(rs -> null);

      System.out.println("Done.");
    }
  }
}
