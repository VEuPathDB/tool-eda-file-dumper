package org.veupathdb.eda.binaryfiles.dumper;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;

import java.util.Map;

public class IdFilesDumperFactory {
  private BinaryFilesManager bfm;
  private Study study;
  private Entity entity;
  private Entity parentEntity;

  public IdFilesDumperFactory(BinaryFilesManager bfm,
                              Study study,
                              Entity entity,
                              Entity parentEntity) {
    this.bfm = bfm;
    this.study = study;
    this.entity = entity;
    this.parentEntity = parentEntity;
  }

  /**
   * Supplies an implementation of an ID FilesDumper based on the number of ancestors of entity member and the
   * byte count allocated for IDs in ancestry.
   */
  public FilesDumper create(Map<String, Integer> bytesReservedForIdByEntityId) {
    if (entity.getAncestorEntities().size() == 1) {
      return new IdFilesDumperOneAncestor(bfm, study, entity, parentEntity, bytesReservedForIdByEntityId);
    } else if (entity.getAncestorEntities().size() > 1) {
      return new IdFilesDumperMultiAncestor(bfm, study, entity, parentEntity, bytesReservedForIdByEntityId);
    } else { // No ancestors
      return new IdFilesDumperNoAncestor(bfm, study, entity, bytesReservedForIdByEntityId.get(entity.getId()));
    }
  }
}
