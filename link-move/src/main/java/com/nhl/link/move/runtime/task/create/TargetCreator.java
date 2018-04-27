package com.nhl.link.move.runtime.task.create;

import com.nhl.link.move.runtime.task.SourceTargetTuple;
import com.nhl.link.move.writer.TargetPropertyWriter;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @param <T>
 * @since 2.6
 */
public class TargetCreator<T extends DataObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetCreator.class);

    private Class<T> type;
    private TargetPropertyWriterFactory<T> writerFactory;

    public TargetCreator(Class<T> type, TargetPropertyWriterFactory<T> writerFactory) {
        this.type = type;
        this.writerFactory = writerFactory;
    }

    public List<SourceTargetTuple<T>> create(ObjectContext context, Collection<Map<String, Object>> sources) {

        List<SourceTargetTuple<T>> result = new ArrayList<>();

        for (Map<String, Object> s : sources) {

            T t = create(context, type, s);

            result.add(new SourceTargetTuple<>(s, t, true));
        }

        return result;
    }

    protected T create(ObjectContext context, Class<T> type, Map<String, Object> source) {

        T target = context.newObject(type);

        if (source.isEmpty()) {
            return target;
        }

        for (Map.Entry<String, Object> e : source.entrySet()) {
            TargetPropertyWriter writer = writerFactory.getOrCreateWriter(e.getKey());
            if (writer == null) {
                LOGGER.info("Source contains property not mapped in the target: " + e.getKey() + ". Skipping...");
                continue;
            }
            if (writer.willWrite(target, e.getValue())) {
                writer.write(target, e.getValue());
            }
        }

        return target;
    }
}
