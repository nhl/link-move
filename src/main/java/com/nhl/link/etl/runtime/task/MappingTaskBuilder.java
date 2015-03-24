package com.nhl.link.etl.runtime.task;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cayenne.DataObject;
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
import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;

/**
 * A common superclass of task builders for tasks that require mapping targets
 * to sources.
 * 
 * @since 1.3
 */
public abstract class MappingTaskBuilder<T extends DataObject> extends BaseTaskBuilder {

	protected IKeyAdapterFactory keyAdapterFactory;
	protected ITargetCayenneService targetCayenneService;

	protected Class<T> type;
	protected Mapper<T> mapper;
	protected boolean byId;
	protected List<String> keyAttributes;

	public MappingTaskBuilder(Class<T> type, ITargetCayenneService targetCayenneService,
			IKeyAdapterFactory keyAdapterFactory) {

		this.type = type;
		this.targetCayenneService = targetCayenneService;
		this.keyAdapterFactory = keyAdapterFactory;
	}

	protected void setMapper(Mapper<T> mapper) {
		this.byId = false;
		this.mapper = mapper;
		this.keyAttributes = null;
	}

	protected void setMapperAttributeNames(String... keyAttributes) {
		this.byId = false;
		this.mapper = null;
		this.keyAttributes = Arrays.asList(keyAttributes);
	}

	protected void setMapperProperties(Property<?>... matchAttributes) {

		// it will fail later on 'build'; TODO: should we do early argument
		// checking?
		if (matchAttributes == null) {
			return;
		}
		String[] names = new String[matchAttributes.length];
		for (int i = 0; i < matchAttributes.length; i++) {
			names[i] = matchAttributes[i].getName();
		}

		setMapperAttributeNames(names);
	}

	protected void setMapperId(String idProperty) {
		this.byId = true;
		this.mapper = null;
		this.keyAttributes = Collections.singletonList(idProperty);
	}

	protected Mapper<T> createMapper() {

		// not wrapping custom matcher, presuming the user knows what's he's
		// doing and his matcher generates proper keys
		if (this.mapper != null) {
			return this.mapper;
		}

		if (keyAttributes == null) {
			throw new IllegalStateException("'matchBy' or 'matchById' must be set");
		}

		Mapper<T> matcher;

		if (byId) {
			matcher = new IdMapper<>(pkAttribute(), getSingleMatchAttribute());
		} else if (keyAttributes.size() > 1) {
			matcher = new MultiAttributeMapper<>(keyAttributes);
		} else {
			matcher = new AttributeMapper<>(getSingleMatchAttribute());
		}

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

		return new SafeMapKeyMapper<>(matcher, keyAdapter);
	}

	private String pkAttribute() {
		ObjEntity oe = targetCayenneService.entityResolver().getObjEntity(type);
		if (oe == null) {
			throw new EtlRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
		}

		Collection<String> pks = oe.getPrimaryKeyNames();
		if (pks.size() != 1) {
			throw new EtlRuntimeException("Only single-column PK is supported for now. Got " + pks.size()
					+ " for entity: " + oe.getName());
		}

		return pks.iterator().next();
	}

	protected String getSingleMatchAttribute() {
		if (keyAttributes == null || keyAttributes.isEmpty()) {
			return null;
		}
		if (keyAttributes.size() > 1) {
			throw new IllegalStateException("Trying to get a single match attribute but multi key matching is set");
		}
		return keyAttributes.get(0);
	}

	private ObjAttribute getMatchAttribute() {

		ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);

		if (entity == null) {
			throw new IllegalStateException("Type " + type.getName() + " is not mapped in Cayenne");
		}

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
}
