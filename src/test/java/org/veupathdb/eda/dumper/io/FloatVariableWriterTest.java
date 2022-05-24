package org.veupathdb.eda.dumper.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.eda.dumper.io.serializer.FloatVariableSerializer;
import org.veupathdb.eda.dumper.noindex.Variable;
import org.veupathdb.eda.dumper.noindex.VariableReader;
import org.veupathdb.eda.dumper.noindex.VariableWithIdSerializer;
import org.veupathdb.eda.dumper.noindex.VariableWriter;

public class FloatVariableWriterTest {

  @Test
  public void testWriteAndRead() throws Exception {
    final List<Variable<Float>> floatVariables = List.of(
        new Variable<>(0, 1.0f),
        new Variable<>(1, 3.0f),
        new Variable<>(2, 6.0f)
    );

    final int expectedByteArrayLength = 8 * floatVariables.size();
    final VariableWithIdSerializer<Float,Float> floatConverter = new VariableWithIdSerializer<>(new FloatVariableSerializer());

    // Could just as easily be a FileOutputStream if we wanted to write to a file.
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(expectedByteArrayLength);
         final VariableWriter<Float,Float> writer = new VariableWriter<>(outputStream, floatConverter)) {

      // Write each variable to the output stream.
      for (Variable<Float> var : floatVariables) {
        writer.writeVar(var);
      }

      // Fetch what was written to the output stream by the writer.
      final byte[] outputBytes = outputStream.toByteArray();

      // Pipe the output of the writer to the input of the reader.
      try (final InputStream inputStream = new ByteArrayInputStream(outputBytes);
           final VariableReader<Float,Float> reader = new VariableReader<>(inputStream, floatConverter)) {

        final Variable<Float> var1 = reader.readVar();
        Assertions.assertEquals(0, var1.getId());
        Assertions.assertEquals(1.0f, var1.getValue());
        final Variable<Float> var2 = reader.readVar();
        Assertions.assertEquals(1, var2.getId());
        Assertions.assertEquals(3.0f, var2.getValue());
        final Variable<Float> var3 = reader.readVar();
        Assertions.assertEquals(2, var3.getId());
        Assertions.assertEquals(6.0f, var3.getValue());
      }
    }
  }
}
