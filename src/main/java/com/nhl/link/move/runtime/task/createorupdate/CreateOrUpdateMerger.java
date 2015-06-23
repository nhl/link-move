package com.nhl.link.move.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nhl.link.move.EtlRuntimeException;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.writer.TargetPropertyWriter;

/**
 * @since 1.3
 */
public class CreateOrUpdateMerger<T extends DataObject> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CreateOrUpdateMerger.class);

	private Class<T> type;
	private Mapper mapper;
	private Map<String, TargetPropertyWriter> writers;

	public CreateOrUpdateMerger(Class<T> type, Mapper mapper, Map<String, TargetPropertyWriter> writers) {
		this.mapper = mapper;
		this.type = type;
		this.writers = writers;
	}

	public List<CreateOrUpdateTuple<T>> merge(ObjectContext context, Map<Object, Map<String, Object>> mappedSources,
			List<T> matchedTargets) {

		// clone mappedSources as we are planning to truncate it in this method
		Map<Object, Map<String, Object>> localMappedSources = new HashMap<>(mappedSources);

		List<CreateOrUpdateTuple<T>> result = new ArrayList<>();

		for (T t : matchedTargets) {

			Object key = mapper.keyForTarget(t);

			Map<String, Object> src = localMappedSources.remove(key);

			// a null can only mean some algorithm malfunction, as keys are all
			// coming from a known set of sources
			if (src == null) {
				throw new EtlRuntimeException("Invalid key: " + key);
			}

			// skip phantom updates...
			if (update(context, src, t)) {
				result.add(new CreateOrUpdateTuple<>(src, t, false));
			}
		}

		// everything that's left are new objects
		for (Entry<Object, Map<String, Object>> e : localMappedSources.entrySet()) {

			T t = create(context, type, e.getValue());

			result.add(new CreateOrUpdateTuple<>(e.getValue(), t, true));
		}

		return result;
	}

	protected T create(ObjectContext context, Class<T> type, Map<String, Object> source) {
		T target = context.newObject(type);
		update(context, source, target);
		return target;
	}

	protected boolean update(ObjectContext context, Map<String, Object> source, T target) {

		if (source.entrySet().isEmpty()) {
			return false;
		}

		boolean updated = false;

		for (Map.Entry<String, Object> e : source.entrySet()) {
			TargetPropertyWriter writer = writers.get(e.getKey());
			if (writer == null) {
				LOGGER.info("Source contains property not mapped in the target: " + e.getKey() + ". Skipping...");
				continue;
			}

			updated = writer.write(target, e.getValue()) || updated;
		}

		return updated;
	}

}
