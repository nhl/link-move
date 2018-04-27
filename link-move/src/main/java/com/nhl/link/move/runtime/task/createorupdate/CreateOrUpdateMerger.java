package com.nhl.link.move.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nhl.link.move.runtime.task.SourceTargetPair;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.writer.TargetPropertyWriter;

/**
 * @since 1.3
 */
public class CreateOrUpdateMerger<T extends DataObject> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CreateOrUpdateMerger.class);

	private Class<T> type;
	private Mapper mapper;
	private TargetPropertyWriterFactory<T> writerFactory;

	public CreateOrUpdateMerger(Class<T> type, Mapper mapper, TargetPropertyWriterFactory<T> writerFactory) {
		this.mapper = mapper;
		this.type = type;
		this.writerFactory = writerFactory;
	}

	public void merge(List<SourceTargetPair<T>> mapped) {
		for (SourceTargetPair<T> t : mapped) {
			if (!t.isCreated()) {
				merge(t.getSource(), t.getTarget());
			}
		}
	}

	public List<SourceTargetPair<T>> map(ObjectContext context, Map<Object, Map<String, Object>> mappedSources,
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

			// skip phantom updates...
			if (willUpdate(src, t)) {
				result.add(new SourceTargetPair<>(src, t, false));
			}
		}

		// everything that's left are new objects
		for (Map.Entry<Object, Map<String, Object>> e : localMappedSources.entrySet()) {

			T t = create(context, type, e.getValue());

			result.add(new SourceTargetPair<>(e.getValue(), t, true));
		}

		return result;
    }

    protected boolean willUpdate(Map<String, Object> source, T target) {

		if (source.isEmpty()) {
			return false;
		}

		for (Map.Entry<String, Object> e : source.entrySet()) {
			TargetPropertyWriter writer = writerFactory.getOrCreateWriter(e.getKey());
			if (writer == null) {
				LOGGER.info("Source contains property not mapped in the target: " + e.getKey() + ". Skipping...");
				continue;
			}

			if (writer.willWrite(target, e.getValue())) {
                return true;
            }
		}

		return false;
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

	private void merge(Map<String, Object> source, T target) {

		if (source.isEmpty()) {
			return;
		}

		for (Map.Entry<String, Object> e : source.entrySet()) {
			TargetPropertyWriter writer = writerFactory.getOrCreateWriter(e.getKey());
			if (writer == null) {
				LOGGER.info("Source contains property not mapped in the target: " + e.getKey() + ". Skipping...");
				continue;
			}
			writer.write(target, e.getValue());
		}
	}
}
