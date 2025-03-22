package com.nhl.link.move.runtime.task.common;

import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import org.apache.cayenne.Cayenne;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.query.ObjectSelect;
import org.dflib.DataFrame;
import org.dflib.Index;
import org.dflib.row.RowProxy;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.dflib.Exp.$col;

/**
 * Handler of resolve-fk processor stage.
 *
 * @since 2.12
 */
public class FkResolver {

    private TargetEntity targetEntity;

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

            String path = e.getKey().getNormalizedPath();
            Map<Object, Object> fks = e.getValue();

            // TODO: do we care if an FK is invalid (i.e. "get" returns null) ?
            //  For now quietly setting the relationship to null.
            df = df
                    .cols(path).merge($col(path).mapVal(fk -> fk != null ? fks.get(fk) : null));
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
                        .ifPresent(a -> fks.computeIfAbsent(a, ak -> new HashSet<>()).add(val));
            }
        }
    }

    protected void fetchRelated(
            TargetAttribute a,
            Set<Object> fks,
            ObjectContext context,
            Map<TargetAttribute, Map<Object, Object>> related) {

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
    }
}
