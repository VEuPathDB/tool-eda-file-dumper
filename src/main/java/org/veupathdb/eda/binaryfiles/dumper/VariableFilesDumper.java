package org.veupathdb.eda.binaryfiles.dumper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONArray;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager.Operation;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;

public class VariableFilesDumper<T> implements FilesDumper {
  private BinaryValueWriter<VariableValueIdPair<T>> _varFileWriter;
  private AtomicLong _idIndex = new AtomicLong(0);
  private final VariableWithValues<T> _valueVar;
  private final int _valColumnIndex;  // the position in the tabular stream of the var value
  private boolean _firstRow = true;

  public VariableFilesDumper(BinaryFilesManager bfm, Study study, Entity entity, VariableWithValues<T> valueVar) {
    _valueVar = valueVar;
    final File varFile  = bfm.getVariableFile(study, entity, valueVar, Operation.WRITE).toFile();
    _varFileWriter = getVarAndIdBinaryWriter(varFile, valueVar.getBinaryConverter());
    _valColumnIndex = entity.getAncestorEntities().size() + 1;  // row has ancestor IDs followed by this entity's ID, then value
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
    values.forEach(val -> _varFileWriter.writeValue(new VariableValueIdPair<>(idIndex,
        _valueVar.fromString(val.toString()))));
  }

  private void writeSingleValue(List<String> row, Long idIndex) {
    _varFileWriter.writeValue(new VariableValueIdPair<>(idIndex, _valueVar.fromString(row.get(_valColumnIndex))));
  }
}
