package org.veupathdb.eda.binaryfiles.dumper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.veupathdb.eda.binaryfiles.BinaryValueWriter;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.LongitudeVariable;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager.Operation;
import org.veupathdb.service.eda.ss.model.variable.binary.BinarySerializer;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdSerializer;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class VariableFilesStringDumper<T> implements FilesDumper {
  private static final Logger LOG = LogManager.getLogger(VariableFilesStringDumper.class);

  private BinaryValueWriter<VariableValueIdPair<String>> _varFileWriter;
  private AtomicLong _idIndex = new AtomicLong(0);
  private final VariableWithValues<T> _valueVar;
  private final int _valColumnIndex;  // the position in the tabular stream of the var value
  private boolean _firstRow = true;
  private final int maxLen;

  public VariableFilesStringDumper(BinaryFilesManager bfm, Study study, Entity entity, VariableWithValues<T> valueVar) {
    _valueVar = valueVar;
    final File varFile  = bfm.getUtf8VariableFile(study, entity, valueVar, Operation.WRITE).toFile();
    try {
      final OutputStream outputStream = new FileOutputStream(varFile);
      final BinarySerializer<VariableValueIdPair<String>> valueWithIdSerializer = new ValueWithIdSerializer<>(_valueVar.getStringConverter());
      maxLen = valueWithIdSerializer.numBytes() - 8;
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
    if (row.get(_valColumnIndex).length() > maxLen - 4) {
      LOG.error("Failed to dump " + row + " var " + _valueVar.getId());
    }
    if (_valueVar.getType() == VariableType.LONGITUDE) {
      Double d = Double.valueOf(row.get(_valColumnIndex));
      BigDecimal bd = BigDecimal.valueOf(d);
      LongitudeVariable var = (LongitudeVariable) _valueVar;
      bd = bd.setScale(Math.min(bd.precision() - 1, var.getPrecision().intValue() - 2), RoundingMode.HALF_UP);
      if (bd.precision() == var.getPrecision().intValue() - 2) {
        LOG.info("Writing max precision: " + var.getId());
      }
      _varFileWriter.writeValue(new VariableValueIdPair<>(idIndex, bd.toString()));
    } else {
      _varFileWriter.writeValue(new VariableValueIdPair<>(idIndex, row.get(_valColumnIndex)));
    }
  }
}
