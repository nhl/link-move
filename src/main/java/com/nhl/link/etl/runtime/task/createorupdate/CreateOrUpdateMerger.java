package com.nhl.link.etl.runtime.task.createorupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.mapper.Mapper;

/**
 * @since 1.3
 */
public class CreateOrUpdateMerger<T extends DataObject> {

	private Class<T> type;
	private Mapper mapper;
	private CreateOrUpdateStrategy<T> createOrUpdateStrategy;

	public CreateOrUpdateMerger(Class<T> type, Mapper mapper, CreateOrUpdateStrategy<T> createOrUpdateStrategy) {
		this.mapper = mapper;
		this.type = type;
		this.createOrUpdateStrategy = createOrUpdateStrategy;
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
			if (createOrUpdateStrategy.update(context, src, t)) {
				result.add(new CreateOrUpdateTuple<>(src, t, false));
			}
		}

		// everything that's left are new objects
		for (Entry<Object, Map<String, Object>> e : localMappedSources.entrySet()) {

			T t = createOrUpdateStrategy.create(context, type, e.getValue());

			result.add(new CreateOrUpdateTuple<>(e.getValue(), t, true));
		}

		return result;
	}

}
