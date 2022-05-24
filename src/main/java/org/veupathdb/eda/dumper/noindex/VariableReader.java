package org.veupathdb.eda.dumper.noindex;

import java.io.IOException;
import java.io.InputStream;

public class VariableReader<S,T> implements AutoCloseable {
  private final InputStream inputStream;
  private final VariableWithIdSerializer<S,T> variableSerializer;

  public VariableReader(InputStream inputStream, VariableWithIdSerializer<S,T> byteConverter) {
    this.inputStream = inputStream;
    this.variableSerializer = byteConverter;
  }

  /**
   * Reads a float variable from the wrapped input stream.
   * @return The next float variable from the input stream.
   * @throws IOException
   */
  public Variable<T> readVar() throws IOException {
    byte[] bytes = new byte[variableSerializer.totalBytesNeeded()];
    inputStream.read(bytes);
    return variableSerializer.covertFromBytes(bytes);
  }

  @Override
  public void close() throws Exception {
    inputStream.close();
  }
}
