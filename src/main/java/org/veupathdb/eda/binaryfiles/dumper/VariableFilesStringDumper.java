package org.veupathdb.eda.binaryfiles.dumper;

import org.json.JSONArray;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager.Operation;
import org.veupathdb.service.eda.subset.model.variable.binary.BinarySerializer;
import org.veupathdb.service.eda.subset.model.variable.binary.StringValueConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.ValueWithIdSerializer;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class VariableFilesStringDumper<T> implements FilesDumper {
  private BinaryValueWriter<VariableValueIdPair<String>> _varFileWriter;
  private AtomicLong _idIndex = new AtomicLong(0);
  private final VariableWithValues<T> _valueVar;
  private final int _valColumnIndex;  // the position in the tabular stream of the var value
  private boolean _firstRow = true;

  public VariableFilesStringDumper(BinaryFilesManager bfm, Study study, Entity entity, VariableWithValues<T> valueVar) {
    _valueVar = valueVar;
    final File varFile  = bfm.getUtf8VariableFile(study, entity, valueVar, Operation.WRITE).toFile();
    try {
      final OutputStream outputStream = new FileOutputStream(varFile);
      final BinarySerializer<VariableValueIdPair<String>> valueWithIdSerializer = new ValueWithIdSerializer<>(_valueVar.getStringConverter());
      _varFileWriter = new BinaryValueWriter<>(outputStream, valueWithIdSerializer);
      _valColumnIndex = entity.getAncestorEntities().size() + 1;  // row has ancestor IDs followed by this entity's ID, then value
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
    if (_firstRow) { _firstRow = false; return; } // skip header

    Long idIndex = _idIndex.getAndIncrement();              // unconditionally increment the id index
    if (row.get(_valColumnIndex).equals("")) return;        // but don't output missing data rows
    if (_valueVar.getIsMultiValued()) {
      writeMultiValue(row, idIndex);
    } else {
      writeSingleValue(row, idIndex);
    }
  }

  @Override
  public void close() throws Exception {
    _varFileWriter.close();
  }

  private void writeMultiValue(List<String> row, Long idIndex) {
    JSONArray values = new JSONArray(row.get(_valColumnIndex));
    values.forEach(val -> _varFileWriter.writeValue(new VariableValueIdPair<>(idIndex, val.toString())));
  }

  private void writeSingleValue(List<String> row, Long idIndex) {
    _varFileWriter.writeValue(new VariableValueIdPair<>(idIndex, row.get(_valColumnIndex)));
  }
}
