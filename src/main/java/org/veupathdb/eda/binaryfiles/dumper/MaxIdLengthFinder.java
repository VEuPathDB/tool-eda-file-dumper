package org.veupathdb.eda.binaryfiles.dumper;

import org.veupathdb.service.eda.ss.model.tabular.TabularResponses;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MaxIdLengthFinder implements TabularResponses.ResultConsumer {
  private static final int INDEX_OF_ID = 0;
  private int maxLength = 0;
  private boolean done = false;
  private boolean firstRow = true;

  @Override
  public void consumeRow(List<String> record) throws IOException {
    if (firstRow) { firstRow = false; return; } // skip header

    int numBytes = record.get(INDEX_OF_ID).getBytes(StandardCharsets.UTF_8).length + 4;
      if (numBytes > maxLength) {
        System.out.println("Found max length string: " + numBytes + " , " + record.get(INDEX_OF_ID));
        maxLength = numBytes;
      }
  }

  @Override
  public void end() {
    done = true;
  }

  public int getMaxLength() {
    if (done) {
      return maxLength;
    } else {
      throw new IllegalStateException("This instance has not completed it's processing to find the result's max ID length.");
    }
  }
}
