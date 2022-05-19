package org.veupathdb.eda.dumper.io;

import org.veupathdb.eda.dumper.model.VariableDataPoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AsyncVariableFileReader<T> implements AutoCloseable {
  private static final int BUFFER_SIZE = 8192;

  private AsynchronousFileChannel fileChannel;
  private VariableSerializer<T> serializer;
  private BufferResult buffer1;
  private BufferResult buffer2;
  private int filePosition = 0;
  private int bufferPosition = 0;
  private BufferResult nextResult;

  public AsyncVariableFileReader(Path path, VariableSerializer<T> serializer) throws IOException {
    this.fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
    this.buffer1 = new BufferResult();
    this.buffer2 = new BufferResult();

    this.serializer = serializer;
    this.buffer1.read(fileChannel, filePosition);
    this.filePosition += BUFFER_SIZE;
    this.buffer2.read(fileChannel, filePosition);
    this.filePosition += BUFFER_SIZE;
    this.nextResult = buffer1;
  }

  public VariableDataPoint<T> readVariable() {
    try {
      if (bufferPosition >= BUFFER_SIZE) {
        // Once we hit the end of the buffer,
        setNextBuffer();
        bufferPosition = 0;
      }
      // Block until our next result is available.
      final int bytesRead = nextResult.result.get();
      if (!nextResult.hasBeenRead) {
        nextResult.buffer.rewind();
        nextResult.hasBeenRead = true;
      }
      if (bytesRead == -1) {
        return null;
      }
      bufferPosition += serializer.totalBytesNeeded();
      return serializer.convertFromBuffer(nextResult.buffer);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private void setNextBuffer() {
    if (nextResult == buffer1) {
      buffer1.buffer.rewind();
      buffer1.read(fileChannel, filePosition);
      filePosition += BUFFER_SIZE;
      nextResult = buffer2; // Already has data loaded or begun loading.
    } else {
      buffer2.buffer.rewind();
      buffer2.read(fileChannel, filePosition);
      filePosition += BUFFER_SIZE;
      nextResult = buffer1; // Already has data loaded or begun loading.
    }
  }

  @Override
  public void close() throws Exception {
    this.fileChannel.close();
  }

  private static class BufferResult {
    private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private Future<Integer> result;
    private boolean hasBeenRead;

    public void read(AsynchronousFileChannel fileChannel, int pos) {
      hasBeenRead = false;
      result = fileChannel.read(buffer, pos);
    }
  }
}
