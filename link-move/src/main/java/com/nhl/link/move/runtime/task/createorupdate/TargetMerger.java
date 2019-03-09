package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.IndexPosition;
import com.nhl.dflib.row.RowBuilder;
import com.nhl.dflib.row.RowProxy;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.query.ObjectSelect;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @since 2.6
 */
public class TargetMerger<T extends DataObject> {

    private TargetEntity targetEntity;
    private TargetPropertyWriterFactory<T> writerFactory;

    public TargetMerger(TargetEntity targetEntity, TargetPropertyWriterFactory<T> writerFactory) {
        this.writerFactory = writerFactory;
        this.targetEntity = targetEntity;
    }

    public DataFrame merge(ObjectContext context, DataFrame df) {

        Index sourceSubIndex = df
                .getColumns()
                .dropNames(CreateOrUpdateSegment.TARGET_COLUMN, CreateOrUpdateSegment.TARGET_CREATED_COLUMN);

        Map<TargetAttribute, Set<Object>> fks = collectFks(df, sourceSubIndex);
        Map<TargetAttribute, Map<Object, Object>> related = fetchRelated(context, fks);

        Index changeTrackingIndex = df.getColumns().addNames(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN);

        return df
                .map((f, t) -> resolveFks(f, t, related))
                .map(changeTrackingIndex, (f, t) -> merge(f, t, sourceSubIndex))
                .filter(r -> (boolean) r.get(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN))
                .dropColumns(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN);
    }


    private void merge(RowProxy from, RowBuilder to, Index sourceSubIndex) {

        boolean changed = (boolean) from.get(CreateOrUpdateSegment.TARGET_CREATED_COLUMN);
        T target = (T) from.get(CreateOrUpdateSegment.TARGET_COLUMN);

        from.copy(to);

        for (IndexPosition ip : sourceSubIndex) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(ip.name());

            Object val = from.get(ip.ordinal());
            if (writer.willWrite(target, val)) {
                changed = true;
                writer.write(target, val);
            }
        }

        to.set(CreateOrUpdateSegment.TARGET_CHANGED_COLUMN, changed);
    }

    private Map<TargetAttribute, Set<Object>> collectFks(DataFrame df, Index sourceSubIndex) {

        // using TargetAttribute as a map key will work reliably only if sources already have normalized paths
        Map<TargetAttribute, Set<Object>> fks = new HashMap<>();

        df.forEach(r -> {
            for (IndexPosition ip : sourceSubIndex.getPositions()) {
                Object val = r.get(ip.ordinal());

                if (val != null) {
                    targetEntity
                            .getAttribute(ip.name())
                            .filter(a -> a.getForeignKey().isPresent())
                            .ifPresent(a -> fks.computeIfAbsent(a, ak -> new HashSet<>()).add(val));
                }
            }
        });

        return fks;
    }

    private Map<TargetAttribute, Map<Object, Object>> fetchRelated(
            ObjectContext context,
            Map<TargetAttribute, Set<Object>> fkMap) {

        if (fkMap.isEmpty()) {
            return Collections.emptyMap();
        }

        // map of *path* to map of *pk* to *object*...
        Map<TargetAttribute, Map<Object, Object>> related = new HashMap<>();

        fkMap.forEach((a, fks) -> {

            if (!fks.isEmpty()) {

                TargetAttribute relatedPk = a.getForeignKey().get().getTarget();
                String relatedPath = relatedPk.getNormalizedPath().substring(ASTDbPath.DB_PREFIX.length());
                String relatedEntityName = relatedPk.getEntity().getName();

                Map<Object, Object> relatedFetched = new HashMap<>((int) (fks.size() / 0.75));
                ObjectSelect
                        .query(Object.class)
                        .entityName(relatedEntityName)
                        .where(ExpressionFactory.inDbExp(relatedPath, fks))
                        .select(context)
                        // map by fk value (which is a PK of the matched object of course)
                        .forEach(r -> relatedFetched.put(Cayenne.pkForObject((Persistent) r), r));

                related.put(a, relatedFetched);
            }
        });

        return related;
    }

    private void resolveFks(
            RowProxy from,
            RowBuilder to,
            Map<TargetAttribute, Map<Object, Object>> related) {

        from.copy(to);

        related.forEach((attribute, fks) -> {

            String path = attribute.getNormalizedPath();
            Object fk = from.get(path);

            // TODO: do we care if an FK is invalid (i.e. "get" returns null) ? For now quietly setting the
            //  relationship to null.
            to.set(path, fk != null ? fks.get(fk) : null);
        });
    }
}
