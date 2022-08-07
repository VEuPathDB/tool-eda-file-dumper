package org.veupathdb.eda.binaryfiles.dumper;
import java.util.List;

/**
 * map of an entity's idIndex to its entity_id and its ancestor's entity_ids
 * @author sfischer
 *
 */
public class IdsMap {
  private long idIndex;
  private String entityId;
  private List<String> ancestorIds;
  
  public IdsMap(long idIndex, String entityId, List<String> ancestorIds) {
    this.idIndex = idIndex;
    this.entityId = entityId;
    this.ancestorIds = ancestorIds;
  }

  public long getIdIndex() {
    return idIndex;
  }

  public String getEntityId() {
    return entityId;
  }

  public List<String> getAncestorIds() {
    return ancestorIds;
  }

  public boolean equals(IdsMap idsMap) {
    return (idsMap.idIndex == idIndex 
        && idsMap.entityId.equals(entityId)
        && idsMap.ancestorIds.equals(ancestorIds));
  }
  
}
