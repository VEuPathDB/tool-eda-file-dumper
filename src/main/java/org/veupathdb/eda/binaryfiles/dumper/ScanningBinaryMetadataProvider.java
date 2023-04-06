package org.veupathdb.eda.binaryfiles.dumper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.db.FilteredResultFactory;
import org.veupathdb.service.eda.ss.model.reducer.BinaryMetadataProvider;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.*;
import org.veupathdb.service.eda.ss.model.variable.binary.EmptyBinaryProperties;

import javax.sql.DataSource;
import java.util.*;

/**
 * Provides study metadata needed for binary encoding by scanning the database. This implementation is intended for use
 * when the binary files are being generated, since the metadata file is not available.
 */
public class ScanningBinaryMetadataProvider implements BinaryMetadataProvider {
    private static final Logger LOG = LogManager.getLogger(StudyDumper.class);

    private Study study;
    private DataSource dataSource;
    private String schema;

    public ScanningBinaryMetadataProvider(Study study, DataSource dataSource, String schema) {
        this.study = study;
        this.dataSource = dataSource;
        this.schema = schema;
    }

    @Override
    public Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId) {
        try {
            Entity fullyPopulatedEntity = study.getEntity(entity.getId())
                .orElseThrow();
            Variable var = fullyPopulatedEntity
                .getVariable(variableId)
                .orElseThrow();
            if (var instanceof VariableWithValues) {
                VariableWithValues varWithValues = (VariableWithValues) var;
                if (varWithValues.getType() == VariableType.STRING) {
                    final MaxLengthFinder maxLengthFinder = new MaxLengthFinder(fullyPopulatedEntity.getAncestorEntities().size() + 1);
                    FilteredResultFactory.produceTabularSubset(dataSource, schema, study, fullyPopulatedEntity, List.of(varWithValues), List.of(), new TabularReportConfig(), maxLengthFinder);
                    return Optional.of(new StringVariable.StringBinaryProperties(maxLengthFinder.getMaxLength()));
                } else {
                    return Optional.of(new EmptyBinaryProperties());
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("Failed to find metadata for study {} entity {} variable {}", studyAbbrev, entity.getId(), variableId);
            return Optional.empty();
        }
    }
}
