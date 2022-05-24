package org.veupathdb.eda.dumper.io;

import org.veupathdb.service.eda.ss.model.tabular.TabularResponses.ResultConsumer;

public interface FilesDumper extends ResultConsumer, AutoCloseable {

  // no additional methods; just want to join these two interfaces

}
