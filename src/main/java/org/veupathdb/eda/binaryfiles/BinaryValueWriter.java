package org.veupathdb.eda.binaryfiles;

import org.veupathdb.service.eda.ss.model.variable.binary.BinarySerializer;

import java.io.IOException;
import java.io.OutputStream;

public class BinaryValueWriter<T> implements AutoCloseable {

  private final OutputStream outputStream;
  private final BinarySerializer<T> binarySerializer;

  public BinaryValueWriter(final OutputStream outputStream, final BinarySerializer<T> byteSerializer) {
    this.outputStream = outputStream;
    this.binarySerializer = byteSerializer;
  }

  public void writeValue(T variable) {
    try {
      outputStream.write(binarySerializer.toBytes(variable));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this.outputStream.close();
  }
}
