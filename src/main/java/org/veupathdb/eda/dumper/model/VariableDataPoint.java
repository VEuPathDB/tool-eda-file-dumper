package org.veupathdb.eda.dumper.model;

public class VariableDataPoint<T> {
  public int id;
  public T value;

  public VariableDataPoint(int id, T value) {
    this.id = id;
    this.value = value;
  }

  public int getId() {
    return id;
  }

  public T getValue() {
    return value;
  }

}
