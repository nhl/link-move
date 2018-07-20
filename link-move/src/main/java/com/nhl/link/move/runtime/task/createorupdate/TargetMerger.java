package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.task.SourceTargetPair;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.query.ObjectSelect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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

    public List<SourceTargetPair<T>> merge(ObjectContext context, List<SourceTargetPair<T>> mapped) {

        List<SourceTargetPair<T>> mappedWithResolvedFks = resolveFks(context, mapped);

        // merged list may be shorter as we are excluding phantom updates...
        List<SourceTargetPair<T>> merged = new ArrayList<>(mapped.size());

        for (SourceTargetPair<T> t : mappedWithResolvedFks) {
            if (merge(t)) {
                merged.add(t);
            }
        }

        return merged;
    }

    protected List<SourceTargetPair<T>> resolveFks(ObjectContext context, List<SourceTargetPair<T>> withUnresolvedFks) {
        Map<TargetAttribute, Set<Object>> fks = collectFks(withUnresolvedFks.stream().map(SourceTargetPair::getSource));
        Map<TargetAttribute, Map<Object, Object>> related = fetchRelated(context, fks);
        return resolveFks(withUnresolvedFks, related);
    }

    /**
     * @return true if the target has changed as a result of the merge.
     */
    private boolean merge(SourceTargetPair<T> pair) {

        boolean changed = pair.isCreated();

        if (pair.getSource().isEmpty()) {
            return changed;
        }

        T target = pair.getTarget();

        for (Map.Entry<String, Object> e : pair.getSource().entrySet()) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(e.getKey());

            if (writer.willWrite(target, e.getValue())) {
                changed = true;
                writer.write(target, e.getValue());
            }
        }

        return changed;
    }

    private Map<TargetAttribute, Set<Object>> collectFks(Stream<Map<String, Object>> sources) {

        // using ForeignKey as a map key will work reliably only if sources already have normalized paths
        Map<TargetAttribute, Set<Object>> fks = new HashMap<>();

        sources.forEach(s ->
                s.forEach((k, v) -> {

                    if(v != null) {
                        targetEntity
                                .getAttribute(k)
                                .filter(a -> a.getForeignKey().isPresent())
                                .ifPresent(a -> fks.computeIfAbsent(a, ak -> new HashSet<>()).add(v));
                    }
                }));

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

    private List<SourceTargetPair<T>> resolveFks(
            List<SourceTargetPair<T>> unresolved,
            Map<TargetAttribute, Map<Object, Object>> related) {

        if (related.isEmpty()) {
            return unresolved;
        }

        List<SourceTargetPair<T>> resolved = new ArrayList<>(unresolved.size());

        for (SourceTargetPair<T> pair : unresolved) {
            Map<String, Object> originalSrc = pair.getSource();
            Map<String, Object> resolvedSrc = new HashMap<>(pair.getSource());

            related.forEach((attribute, resolvedByFk) -> {

                String path = attribute.getNormalizedPath();

                Object fk = originalSrc.get(path);
                if (fk != null) {
                    Object fkObject = resolvedByFk.get(fk);

                    // TODO: do we care if an FK is invalid (i.e. 'fkObject' is null) ? The old impl just quietly set the
                    // relationship to null. Doing the same here...
                    resolvedSrc.put(path, fkObject);
                }
            });

            resolved.add(new SourceTargetPair<>(resolvedSrc, pair.getTarget(), pair.isCreated()));
        }

        return resolved;
    }
}
