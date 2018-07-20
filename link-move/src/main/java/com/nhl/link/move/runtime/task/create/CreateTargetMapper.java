package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.runtime.task.SourceTargetPair;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @param <T>
 * @since 2.6
 */
public class CreateTargetMapper<T extends DataObject> {

    private Class<T> type;

    public CreateTargetMapper(Class<T> type) {
        this.type = type;
    }

    public List<SourceTargetPair<T>> map(ObjectContext context, Collection<Map<String, Object>> sources) {

        List<SourceTargetPair<T>> result = new ArrayList<>(sources.size());

        for (Map<String, Object> s : sources) {
            T t = create(context, type, s);
            result.add(new SourceTargetPair<>(s, t, true));
        }

        return result;
    }

    protected T create(ObjectContext context, Class<T> type, Map<String, Object> source) {
        return context.newObject(type);
    }
}
