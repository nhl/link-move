package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.runtime.task.SourceTargetPair;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.Persistent;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.query.ObjectSelect;
import org.apache.cayenne.reflect.ToOneProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @since 2.6
 */
public class TargetMerger<T extends DataObject> {

    private TargetPropertyWriterFactory<T> writerFactory;

    public TargetMerger(TargetPropertyWriterFactory<T> writerFactory) {
        this.writerFactory = writerFactory;
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
        Map<ToOneProperty, Set<Object>> fks = collectFks(withUnresolvedFks.stream().map(SourceTargetPair::getSource));
        Map<String, Map<Object, Object>> related = fetchRelated(context, fks);
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

    private Map<ToOneProperty, Set<Object>> collectFks(Stream<Map<String, Object>> sources) {

        // TODO: implement me
    }

    private Map<String, Map<Object, Object>> fetchRelated(ObjectContext context, Map<ToOneProperty, Set<Object>> fkMap) {

        if (fkMap.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Map<Object, Object>> related = new HashMap<>();

        fkMap.forEach((property, fks) -> {

            if (!fks.isEmpty()) {

                Map<Object, Object> relatedForProperty = new HashMap<>((int) (fks.size() / 0.75));
                ObjectSelect
                        .query(property.getTargetDescriptor().getObjectClass())
                        // TODO: PK IN expression
                        .where()
                        .select(context)
                        // map by fk value (which is a PK of the matched object of course)
                        .forEach(r -> relatedForProperty.put(Cayenne.pkForObject((Persistent) r), r));
            }
        });

        return related;
    }

    private List<SourceTargetPair<T>> resolveFks(
            List<SourceTargetPair<T>> unresolved,
            Map<String, Map<Object, Object>> related) {

        if (related.isEmpty()) {
            return unresolved;
        }

        List<SourceTargetPair<T>> resolved = new ArrayList<>(unresolved.size());

        for (SourceTargetPair<T> pair : unresolved) {
            Map<String, Object> originalSrc = pair.getSource();
            Map<String, Object> resolvedSrc = new HashMap<>(pair.getSource());

            related.forEach((property, resolvedByFk) -> {

                Object fk = originalSrc.get(property);
                if (fk != null) {
                    Object fkObject = resolvedByFk.get(fk);

                    // TODO: do we care if an FK is invalid (i.e. 'fkObject' is null) ? The old impl just quietly set the
                    // relationship to null. Doing the same here...
                    resolvedSrc.put(property, fkObject);
                }
            });

            resolved.add(new SourceTargetPair<>(resolvedSrc, pair.getTarget(), pair.isCreated()));
        }

        return resolved;
    }
}
