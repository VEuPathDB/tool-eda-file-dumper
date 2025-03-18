package org.veupathdb.eda.binaryfiles.dumper;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IdFilesDumperFactory {
  private BinaryFilesManager bfm;
  private Study study;
  private Entity entity;
  private Entity parentEntity;
  private ExecutorService threadPool;

  public IdFilesDumperFactory(BinaryFilesManager bfm,
                              Study study,
                              Entity entity,
                              Entity parentEntity) {
    this.bfm = bfm;
    this.study = study;
    this.entity = entity;
    this.parentEntity = parentEntity;
    this.threadPool = Executors.newCachedThreadPool();
  }

  /**
   * Supplies an implementation of an ID FilesDumper based on the number of ancestors of entity member and the
   * byte count allocated for IDs in ancestry.
   */
  public FilesDumper create(Map<String, Integer> bytesReservedForIdByEntityId) {
    switch (entity.getAncestorEntities().size()) {
      case 0: return new IdFilesDumperNoAncestor(bfm, study, entity, bytesReservedForIdByEntityId.get(entity.getId()));
      case 1: return new IdFilesDumperOneAncestor(bfm, study, entity, parentEntity, bytesReservedForIdByEntityId, threadPool);
      default: return new IdFilesDumperMultiAncestor(bfm, study, entity, parentEntity, bytesReservedForIdByEntityId, threadPool);
    }
  }
}
