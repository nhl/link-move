package com.nhl.link.move.runtime.task.common;

import com.nhl.link.move.runtime.targetmodel.ForeignKey;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import org.apache.cayenne.Cayenne;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.query.ObjectSelect;
import org.dflib.DataFrame;
import org.dflib.Index;
import org.dflib.row.RowProxy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handler of resolve-fk processor stage.
 *
 * @since 2.12
 */
public class FkResolver {

    private final TargetEntity targetEntity;

    public FkResolver(TargetEntity targetEntity) {
        this.targetEntity = targetEntity;
    }

    public DataFrame resolveFks(ObjectContext context, DataFrame df) {
        Map<TargetAttribute, Set<Object>> fks = collectFks(df, ProcessorUtil.dataColumns(df));
        Map<TargetAttribute, Map<Object, Object>> related = fetchRelated(context, fks);

        return resolveFks(df, related);
    }

    protected Map<TargetAttribute, Set<Object>> collectFks(DataFrame df, Index valueColumns) {
        // using TargetAttribute as a map key will work reliably only if sources already have normalized paths
        Map<TargetAttribute, Set<Object>> fks = new HashMap<>();
        df.forEach(r -> collectFks(r, valueColumns, fks));
        return fks;
    }

    protected Map<TargetAttribute, Map<Object, Object>> fetchRelated(
            ObjectContext context,
            Map<TargetAttribute, Set<Object>> fkMap) {

        if (fkMap.isEmpty()) {
            return Collections.emptyMap();
        }

        // map of *path* to map of *pk* to *object*...
        Map<TargetAttribute, Map<Object, Object>> related = new HashMap<>();
        fkMap.forEach((a, fks) -> fetchRelated(a, fks, context, related));
        return related;
    }

    protected DataFrame resolveFks(
            DataFrame df,
            Map<TargetAttribute, Map<Object, Object>> related) {

        for (Map.Entry<TargetAttribute, Map<Object, Object>> e : related.entrySet()) {

            ForeignKey fk = e.getKey().getForeignKey().get();
            List<String> sourceColumns = fk.getSourceColumns();
            Map<Object, Object> fks = e.getValue();

            // Append a column named after the relationship holding the resolved related object, leaving
            // the raw FK source columns intact. A single-column FK is just a one-column tuple, so the
            // same row lookup covers both cases.
            // TODO: do we care if an FK is invalid (i.e. "get" returns null) ?
            //  For now quietly setting the relationship to null.
            df = df
                    .colsAppend(fk.getResultColumn())
                    .merge(row -> {
                        Object key = keyForRow(sourceColumns, row);
                        return key != null ? fks.get(key) : null;
                    });
        }

        return df;
    }

    protected void collectFks(RowProxy row, Index valueColumns, Map<TargetAttribute, Set<Object>> fks) {
        for (String label : valueColumns) {
            Object val = row.get(valueColumns.position(label));

            if (val != null) {
                targetEntity
                        .getAttribute(label)
                        .filter(a -> a.getForeignKey().isPresent())
                        .ifPresent(a -> {
                            // for a multi-column FK the FK-owning column's value alone is not enough -
                            // build a key across all of the FK's source columns
                            Object key = keyForRow(a.getForeignKey().get().getSourceColumns(), row);
                            if (key != null) {
                                fks.computeIfAbsent(a, ak -> new HashSet<>()).add(key);
                            }
                        });
            }
        }
    }

    protected void fetchRelated(
            TargetAttribute a,
            Set<Object> fks,
            ObjectContext context,
            Map<TargetAttribute, Map<Object, Object>> related) {

        if (fks.isEmpty()) {
            return;
        }

        ForeignKey fk = a.getForeignKey().get();
        if (fk.isMultiColumn()) {
            fetchRelatedMultiColumn(a, fk, fks, context, related);
        } else {
            fetchRelatedSingleColumn(a, fk, fks, context, related);
        }
    }

    private void fetchRelatedSingleColumn(
            TargetAttribute a,
            ForeignKey fk,
            Set<Object> fks,
            ObjectContext context,
            Map<TargetAttribute, Map<Object, Object>> related) {

        TargetAttribute relatedPk = fk.getTargets().getFirst();
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

    private void fetchRelatedMultiColumn(
            TargetAttribute a,
            ForeignKey fk,
            Set<Object> fks,
            ObjectContext context,
            Map<TargetAttribute, Map<Object, Object>> related) {

        List<TargetAttribute> targets = fk.getTargets();
        List<String> targetPkColumns = new ArrayList<>(targets.size());
        for (TargetAttribute t : targets) {
            targetPkColumns.add(t.getNormalizedPath().substring(ASTDbPath.DB_PREFIX.length()));
        }
        String relatedEntityName = targets.getFirst().getEntity().getName();

        // qualifier is an OR of per-tuple matches, each matching all PK columns of the target
        Expression qualifier = null;
        for (Object tupleObj : fks) {
            List<?> tuple = (List<?>) tupleObj;

            Expression tupleMatch = null;
            for (int i = 0; i < targetPkColumns.size(); i++) {
                Expression m = ExpressionFactory.matchDbExp(targetPkColumns.get(i), tuple.get(i));
                tupleMatch = tupleMatch == null ? m : tupleMatch.andExp(m);
            }

            qualifier = qualifier == null ? tupleMatch : qualifier.orExp(tupleMatch);
        }

        Map<Object, Object> relatedFetched = new HashMap<>((int) (fks.size() / 0.75));
        ObjectSelect
                .query(Object.class)
                .entityName(relatedEntityName)
                .where(qualifier)
                // map by the matched object's PK tuple (which equals the collected FK tuple)
                .select(context)
                .forEach(r -> relatedFetched.put(pkTuple((Persistent) r, targetPkColumns), r));

        related.put(a, relatedFetched);
    }

    /**
     * Builds the value that identifies a related object for the given FK source columns. Returns a
     * scalar for a single-column FK, a value tuple for a multi-column FK, or null if any component is
     * null (i.e. the relationship can't be resolved).
     */
    private static Object keyForRow(List<String> sourceColumns, RowProxy row) {

        if (sourceColumns.size() == 1) {
            return row.get(sourceColumns.getFirst());
        }

        List<Object> tuple = new ArrayList<>(sourceColumns.size());
        for (String c : sourceColumns) {
            Object v = row.get(c);
            if (v == null) {
                return null;
            }
            tuple.add(v);
        }

        return tuple;
    }

    private static List<Object> pkTuple(Persistent object, List<String> pkColumns) {
        Map<String, Object> id = object.getObjectId().getIdSnapshot();
        List<Object> tuple = new ArrayList<>(pkColumns.size());
        for (String col : pkColumns) {
            tuple.add(id.get(col));
        }
        return tuple;
    }
}
