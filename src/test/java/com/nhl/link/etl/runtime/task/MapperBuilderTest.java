package com.nhl.link.etl.runtime.task;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.mapper.PathMapper;
import com.nhl.link.etl.mapper.Mapper;
import com.nhl.link.etl.mapper.MultiPathMapper;
import com.nhl.link.etl.runtime.key.KeyAdapterFactory;

public class MapperBuilderTest {

	private MapperBuilder builder;

	@Before
	public void before() {

		ObjEntity e = mock(ObjEntity.class);
		builder = new MapperBuilder(e, new KeyAdapterFactory());
	}

	@Test
	public void testBuildUnsafe_ByPropreties_Single() {
		Mapper mapper = builder.matchBy(new Property<Object>("a")).buildUnsafe();
		assertNotNull(mapper);
		assertTrue(mapper instanceof PathMapper);
	}

	@Test
	public void testBuildUnsafe_ByPropreties_Multi() {
		Mapper mapper = builder.matchBy(new Property<Object>("a"), new Property<Object>("b")).buildUnsafe();
		assertNotNull(mapper);
		assertTrue(mapper instanceof MultiPathMapper);
	}
}
