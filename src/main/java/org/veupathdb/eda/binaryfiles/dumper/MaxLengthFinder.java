package org.veupathdb.eda.binaryfiles.dumper;

import org.veupathdb.service.eda.subset.model.tabular.TabularResponses;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * ResultConsumer that tracks the longest ID. Returns the max length once the entire result has been consumed.
 */
public class MaxLengthFinder implements TabularResponses.ResultConsumer {
  private int maxLength = 0;
  private boolean done = false;
  private boolean firstRow = true;
  private final int index;

  public MaxLengthFinder(int columnIndex) {
    this.index = columnIndex;
  }

  @Override
  public void consumeRow(List<String> record) throws IOException {
    if (firstRow) { firstRow = false; return; } // skip header

    // Number of bytes equals bytes in string plus bytes needed to store string length.
    int numBytes = record.get(index).getBytes(StandardCharsets.UTF_8).length + Integer.BYTES;
    if (numBytes > maxLength) {
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
      throw new IllegalStateException("This instance has not completed its processing to find the result's max ID length.");
    }
  }
}
