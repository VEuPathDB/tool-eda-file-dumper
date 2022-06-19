package org.veupathdb.eda.dumper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class VariableFilesDumper<T> implements FilesDumper {
  private BinaryValueWriter<VariableValueIdPair<T>> _varFileWriter;
  private AtomicLong _idIndex = new AtomicLong(0);
  private final VariableWithValues<T> _valueVar;
  private final int _valColumnIndex;  // the position in the tabular stream of the var value

  public VariableFilesDumper(BinaryFilesManager bfm, Study study, Entity entity, VariableWithValues<T> valueVar) {
    _valueVar = valueVar;
    final File varFile  = bfm.getVariableFile(study, entity, valueVar).toFile();
    _varFileWriter = getVarAndIdBinaryWriter(varFile, valueVar.getBinaryConverter());
    _valColumnIndex = entity.getAncestorEntities().size() + 1;  // row has ancestor IDs followed by this entity's ID, then value
  }

  @Override
  public void consumeRow(List<String> row) throws IOException {
    VariableValueIdPair<T> var = new VariableValueIdPair<T>(_idIndex.getAndIncrement(), extractValueFromRow(row));
    _varFileWriter.writeValue(var);
  }

  @Override
  public void close() throws Exception {
    _varFileWriter.close();
  }
  
  private T extractValueFromRow(List<String> row) {
    return _valueVar.fromString(row.get(_valColumnIndex)); 
  }

}
