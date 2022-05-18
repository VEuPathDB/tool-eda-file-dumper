package org.veupathdb.eda.dumper.model;

public class Variable<T> {
  public int id;
  public T value;

  public Variable(int varId, T value) {
    this.id = varId;
    this.value = value;
  }

  public int getId() {
    return id;
  }

  public T getValue() {
    return value;
  }

}
