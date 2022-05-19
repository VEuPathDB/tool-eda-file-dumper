package org.veupathdb.eda.dumper.io;

import org.veupathdb.eda.dumper.model.VariableDataPoint;

import java.io.IOException;
import java.io.OutputStream;

public class VariableWriter<T> implements AutoCloseable {
  private OutputStream outputStream;
  private VariableSerializer<T> variableSerializer;

  public VariableWriter(final OutputStream outputStream, final VariableSerializer byteConverter) {
    this.outputStream = outputStream;
    this.variableSerializer = byteConverter;
  }

  public void writeVar(VariableDataPoint<T> variable) throws IOException {
    outputStream.write(variableSerializer.convertToBytes(variable));
  }

  @Override
  public void close() throws Exception {
    this.outputStream.close();
  }
}
