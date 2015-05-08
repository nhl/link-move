package com.nhl.link.etl.runtime.task;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.mapper.AttributeMapper;
import com.nhl.link.etl.mapper.IdMapper;
import com.nhl.link.etl.mapper.KeyAdapter;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.mapper.MultiAttributeMapper;
import com.nhl.link.etl.mapper.SafeMapKeyMapper;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;

/**
 * @since 1.3
 */
public class MapperBuilder {

	private IKeyAdapterFactory keyAdapterFactory;

	private ObjEntity entity;
	private boolean byId;
	private List<String> keyAttributes;

	public MapperBuilder(ObjEntity entity, IKeyAdapterFactory keyAdapterFactory) {
		this.entity = entity;
		this.keyAdapterFactory = keyAdapterFactory;
	}

	public MapperBuilder matchBy(String... keyAttributes) {
		this.byId = false;
		this.keyAttributes = Arrays.asList(keyAttributes);
		return this;
	}

	public MapperBuilder matchBy(Property<?>... matchAttributes) {

		// it will fail later on 'build'; TODO: should we do early argument
		// checking?
		if (matchAttributes == null) {
			return this;
		}
		String[] names = new String[matchAttributes.length];
		for (int i = 0; i < matchAttributes.length; i++) {
			names[i] = matchAttributes[i].getName();
		}

		return matchBy(names);
	}

	public MapperBuilder matchById(String idProperty) {
		this.byId = true;
		this.keyAttributes = Collections.singletonList(idProperty);
		return this;
	}

	public Mapper build() {

		Mapper mapper = buildUnsafe();

		KeyAdapter keyAdapter;

		// TODO: mapping keyMapAdapters by type doesn't take into account
		// composition and hierarchy of the keys ... need a different approach.
		// for now resorting to the hacks below
		if (keyAttributes.size() > 1) {
			keyAdapter = keyAdapterFactory.adapter(List.class);
		} else {
			ObjAttribute attribute = getMatchAttribute();
			keyAdapter = keyAdapterFactory.adapter(attribute.getJavaClass());
		}

		return new SafeMapKeyMapper(mapper, keyAdapter);
	}

	Mapper buildUnsafe() {

		if (keyAttributes == null) {
			throw new IllegalStateException("'matchBy' or 'matchById' must be set");
		}

		if (byId) {
			return new IdMapper(pkAttribute(), getSingleMatchAttribute());
		} else if (keyAttributes.size() > 1) {
			return new MultiAttributeMapper(keyAttributes);
		} else {
			return new AttributeMapper(getSingleMatchAttribute());
		}
	}

	private String pkAttribute() {

		Collection<String> pks = entity.getPrimaryKeyNames();
		if (pks.size() != 1) {
			throw new EtlRuntimeException("Only single-column PK is supported for now. Got " + pks.size()
					+ " for entity: " + entity.getName());
		}

		return pks.iterator().next();
	}

	public String getSingleMatchAttribute() {
		if (keyAttributes == null || keyAttributes.isEmpty()) {
			return null;
		}
		if (keyAttributes.size() > 1) {
			throw new IllegalStateException("Trying to get a single match attribute but multi key matching is set");
		}
		return keyAttributes.get(0);
	}

	private ObjAttribute getMatchAttribute() {

		if (byId) {
			return entity.getPrimaryKeys().iterator().next();
		} else {
			String matchAttribute = getSingleMatchAttribute();
			ObjAttribute a = entity.getAttribute(matchAttribute);
			if (a == null) {
				throw new IllegalStateException("Invalid attribute name " + matchAttribute + " for entity "
						+ entity.getName());
			}
			return a;
		}
	}

	public boolean isById() {
		return byId;
	}

}
