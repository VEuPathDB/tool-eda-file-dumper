package org.veupathdb.eda.binaryfiles;

import org.veupathdb.service.eda.ss.model.variable.binary.BinaryDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class BinaryValueReader<T> implements AutoCloseable {

  private final InputStream _inputStream;
  private final BinaryDeserializer<T> _binaryDeserializer;

  public BinaryValueReader(final InputStream inputStream, final BinaryDeserializer<T> byteDeserializer) {
    this._inputStream = inputStream;
    this._binaryDeserializer = byteDeserializer;
  }

  public Optional<T> next()  {
    try {
      byte[] result = new byte[_binaryDeserializer.numBytes()];
      int i = _inputStream.read(result);
      if (i == -1) return Optional.empty();
      return Optional.of(_binaryDeserializer.fromBytes(result));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this._inputStream.close();
  }
}

