package org.veupathdb.eda.dumper.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.eda.dumper.model.VariableDataPoint;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class AsyncVariableReaderTest {

  @Test
  public void testWriteAndRead() throws Exception {
    final List<VariableDataPoint<Float>> floatVariables = Arrays.asList(
        new VariableDataPoint(0, 1.0f),
        new VariableDataPoint(1, 3.0f),
        new VariableDataPoint(2, 6.0f)
    );
    final Path file = Path.of("/Users/rooslab/Downloads/binary-output");
    final int expectedByteArrayLength = 8 * floatVariables.size();
    final VariableSerializer<Float> floatConverter = new FloatVariableSerializer();

    // Could just as easily be a FileOutputStream if we wanted to write to a file.
    try (final FileOutputStream outputStream = new FileOutputStream("/Users/rooslab/Downloads/binary-output");
         final VariableWriter<Float> writer = new VariableWriter(outputStream, floatConverter)) {

      // Write each variable to the output stream.
      for (VariableDataPoint<Float> var : floatVariables) {
        writer.writeVar(var);
      }

      // Pipe the output of the writer to the input of the reader.
      try (final AsyncVariableFileReader<Float> reader = new AsyncVariableFileReader(file, floatConverter)) {
        final VariableDataPoint<Float> var1 = reader.readVariable();
        Assertions.assertEquals(0, var1.getId());
        Assertions.assertEquals(1.0f, var1.getValue());
        final VariableDataPoint<Float> var2 = reader.readVariable();
        Assertions.assertEquals(1, var2.getId());
        Assertions.assertEquals(3.0f, var2.getValue());
        final VariableDataPoint<Float> var3 = reader.readVariable();
        Assertions.assertEquals(2, var3.getId());
        Assertions.assertEquals(6.0f, var3.getValue());
      }
    }
  }

  @Test
  public void testWriteAndReadBigFile() throws Exception {
    final Path file = Path.of("/Users/rooslab/Downloads/binary-output");
    final VariableSerializer<Float> floatConverter = new FloatVariableSerializer();

    // Could just as easily be a FileOutputStream if we wanted to write to a file.
    try (final FileOutputStream outputStream = new FileOutputStream("/Users/rooslab/Downloads/binary-output");
         final VariableWriter<Float> writer = new VariableWriter(outputStream, floatConverter)) {

      // Write each variable to the output stream.
      for (int i = 0; i < 100000; i++) {
        writer.writeVar(new VariableDataPoint<>(i, (float) i));
      }

      try (final AsyncVariableFileReader<Float> reader = new AsyncVariableFileReader(file, floatConverter)) {
        for (int i = 0; i < 100000; i++) {
          VariableDataPoint<Float> var = reader.readVariable();
          Assertions.assertEquals(i, var.id);
          Assertions.assertEquals((float) i, var.value);
        }
      }
    }
  }
}
