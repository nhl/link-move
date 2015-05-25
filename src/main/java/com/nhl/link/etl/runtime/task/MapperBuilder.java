package com.nhl.link.etl.runtime.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.Property;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.map.ObjRelationship;

import com.nhl.link.etl.mapper.KeyAdapter;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.mapper.MultiPathMapper;
import com.nhl.link.etl.mapper.PathMapper;
import com.nhl.link.etl.mapper.SafeMapKeyMapper;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;

/**
 * @since 1.3
 */
public class MapperBuilder {

	private IKeyAdapterFactory keyAdapterFactory;

	private ObjEntity entity;
	private List<String> paths;

	public MapperBuilder(ObjEntity entity, IKeyAdapterFactory keyAdapterFactory) {
		this.entity = entity;
		this.keyAdapterFactory = keyAdapterFactory;
	}

	public MapperBuilder matchBy(String... paths) {
		this.paths = Arrays.asList(paths);
		return this;
	}

	public MapperBuilder matchBy(Property<?>... paths) {

		// it will fail later on 'build'; TODO: should we do early argument
		// checking?
		if (paths == null) {
			return this;
		}
		String[] names = new String[paths.length];
		for (int i = 0; i < paths.length; i++) {
			names[i] = paths[i].getName();
		}

		return matchBy(names);
	}

	public MapperBuilder matchById() {

		List<String> pks = new ArrayList<>(3);
		for (DbAttribute pk : entity.getDbEntity().getPrimaryKeys()) {
			pks.add(ASTDbPath.DB_PREFIX + pk.getName());
		}

		if (pks.isEmpty()) {
			throw new IllegalStateException("Target entity has no PKs defined: " + entity.getDbEntityName());
		}

		this.paths = pks;
		return this;
	}

	@SuppressWarnings("deprecation")
	public Mapper build() {

		Mapper mapper = buildUnsafe();

		KeyAdapter keyAdapter;

		if (paths.size() > 1) {
			// TODO: mapping keyMapAdapters by type doesn't take into account
			// composition and hierarchy of the keys ... need a different
			// approach. for now resorting to the hacks below

			keyAdapter = keyAdapterFactory.adapter(List.class);
		} else {

			Object attributeOrRelationship = ExpressionFactory.exp(paths.get(0)).evaluate(entity);

			Class<?> type;

			if (attributeOrRelationship instanceof ObjAttribute) {
				type = ((ObjAttribute) attributeOrRelationship).getJavaClass();
			} else if (attributeOrRelationship instanceof ObjRelationship) {
				type = ((ObjRelationship) attributeOrRelationship).getTargetEntity().getJavaClass();
			} else {
				type = null;
			}

			keyAdapter = keyAdapterFactory.adapter(type);
		}

		return new SafeMapKeyMapper(mapper, keyAdapter);
	}

	Mapper buildUnsafe() {

		if (paths == null) {
			matchById();
		}

		if (paths == null || paths.isEmpty()) {
			throw new IllegalStateException("'matchBy' or 'matchById' must be set");
		}

		Map<String, Mapper> mappers = createMappers();
		return mappers.size() > 1 ? new MultiPathMapper(mappers) : mappers.values().iterator().next();
	}

	private Map<String, Mapper> createMappers() {
		// ensuring predictable attribute iteration order with
		// LinkedHashMap. Useful for unit test for one thing.
		Map<String, Mapper> mappers = new LinkedHashMap<>();
		for (String a : paths) {
			mappers.put(a, new PathMapper(a));
		}

		return mappers;
	}
}
