package org.veupathdb.eda.dumper.io;

import org.veupathdb.service.eda.ss.model.variable.converter.BinarySerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class BinaryValueReader<T> implements AutoCloseable {

  private final InputStream _inputStream;
  private final BinarySerializer<T> _binarySerializer;

  public BinaryValueReader(final InputStream inputStream, final BinarySerializer<T> byteSerializer) {
    this._inputStream = inputStream;
    this._binarySerializer = byteSerializer;
  }

  public Optional<T> next()  {
    try {
      byte[] result = new byte[_binarySerializer.numBytes()];
      int i = _inputStream.read(result);
      if (i == -1) return Optional.empty();
      return Optional.of(_binarySerializer.fromBytes(result));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this._inputStream.close();
  }
}

