package org.veupathdb.eda.dumper.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.eda.dumper.model.Variable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class IntVariableWriterTest {

  @Test
  public void testWriteAndRead() throws Exception {
    final List<Variable<Integer>> intVars = Arrays.asList(
        new Variable(0, 150),
        new Variable(1, 200),
        new Variable(2, 300),
        new Variable(3, 550)
    );
    final int expectedByteArrayLength = 8 * intVars.size();
    final VariableSerializer<Integer> intSerializer = new IntVariableSerializer();

    // Could just as easily be a FileOutputStream if we wanted to write to a file.
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(expectedByteArrayLength);
        final VariableWriter<Integer> writer = new VariableWriter(outputStream, intSerializer)) {

      // Write each variable to the output stream.
      for (Variable<Integer> var : intVars) {
        writer.writeVar(var);
      }

      // Fetch what was written to the output stream by the writer.
      final byte[] outputBytes = outputStream.toByteArray();

      // Pipe the output of the writer to the input of the reader.
      try (final InputStream inputStream = new ByteArrayInputStream(outputBytes);
           final VariableReader<Integer> reader = new VariableReader(inputStream, intSerializer)) {

        final Variable<Integer> var1 = reader.readVar();
        Assertions.assertEquals(0, var1.getId());
        Assertions.assertEquals(150, var1.getValue());
        final Variable<Integer> var2 = reader.readVar();
        Assertions.assertEquals(1, var2.getId());
        Assertions.assertEquals(200, var2.getValue());
        final Variable<Integer> var3 = reader.readVar();
        Assertions.assertEquals(2, var3.getId());
        Assertions.assertEquals(300, var3.getValue());
        final Variable<Integer> var4 = reader.readVar();
        Assertions.assertEquals(3, var4.getId());
        Assertions.assertEquals(550, var4.getValue());
      }
    }
  }
}
