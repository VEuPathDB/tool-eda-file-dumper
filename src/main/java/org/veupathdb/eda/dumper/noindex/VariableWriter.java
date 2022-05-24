package org.veupathdb.eda.dumper.noindex;

import java.io.IOException;
import java.io.OutputStream;

public class VariableWriter<S,T> implements AutoCloseable {

  private OutputStream outputStream;
  private VariableWithIdSerializer<S,T> variableSerializer;

  public VariableWriter(final OutputStream outputStream, final VariableWithIdSerializer<S,T> byteConverter) {
    this.outputStream = outputStream;
    this.variableSerializer = byteConverter;
  }

  public void writeVar(Variable<T> variable) throws IOException {
    outputStream.write(variableSerializer.convertToBytes(variable));
  }

  @Override
  public void close() throws Exception {
    this.outputStream.close();
  }
}
