package org.veupathdb.eda.dumper.io;

import org.veupathdb.eda.dumper.model.Variable;

import java.io.IOException;
import java.io.InputStream;

public class VariableReader<T> implements AutoCloseable {
  private final InputStream inputStream;
  private final VariableSerializer<T> variableSerializer;

  public VariableReader(InputStream inputStream, VariableSerializer<T> byteConverter) {
    this.inputStream = inputStream;
    this.variableSerializer = byteConverter;
  }

  /**
   * Reads a float variable from the wrapped input stream.
   * @return The next float variable from the input stream.
   * @throws IOException
   */
  public Variable<T> readVar() throws IOException {
    byte[] bytes = new byte[8];
    inputStream.read(bytes);
    return variableSerializer.covertFromBytes(bytes);
  }

  @Override
  public void close() throws Exception {
    inputStream.close();
  }
}
