package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.task.SourceTargetPair;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since 2.6
 */
public class TargetMapper<T extends DataObject> {

    private Class<T> type;
    private Mapper mapper;

    public TargetMapper(Class<T> type, Mapper mapper) {
        this.mapper = mapper;
        this.type = type;
    }

    public List<SourceTargetPair<T>> map(
            ObjectContext context,
            Map<Object, Map<String, Object>> mappedSources,
            List<T> matchedTargets) {

        // clone mappedSources as we are planning to truncate it in this method
        Map<Object, Map<String, Object>> localMappedSources = new HashMap<>(mappedSources);

        List<SourceTargetPair<T>> result = new ArrayList<>();

        for (T t : matchedTargets) {

            Object key = mapper.keyForTarget(t);

            Map<String, Object> src = localMappedSources.remove(key);

            // a null can only mean some algorithm malfunction, as keys are all
            // coming from a known set of sources
            if (src == null) {
                throw new LmRuntimeException("Invalid key: " + key);
            }

            // including phantom updates for now... will filter them out during the merge state
            result.add(new SourceTargetPair<>(src, t, false));
        }

        // everything that's left are new objects
        for (Map.Entry<Object, Map<String, Object>> e : localMappedSources.entrySet()) {
            T t = create(context, type);
            result.add(new SourceTargetPair<>(e.getValue(), t, true));
        }

        return result;
    }

    protected T create(ObjectContext context, Class<T> type) {
        return context.newObject(type);
    }
}
