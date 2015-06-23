package com.nhl.link.move.runtime.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.cayenne.exp.Property;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.mapper.MultiPathMapper;
import com.nhl.link.move.mapper.PathMapper;
import com.nhl.link.move.runtime.key.KeyAdapterFactory;
import com.nhl.link.move.runtime.path.EntityPathNormalizer;
import com.nhl.link.move.runtime.task.MapperBuilder;

public class MapperBuilderTest {

	private MapperBuilder builder;

	@Before
	public void before() {

		ObjEntity e = mock(ObjEntity.class);
		EntityPathNormalizer pathNormalizer = mock(EntityPathNormalizer.class);
		when(pathNormalizer.normalize("a")).thenReturn("db:a");
		when(pathNormalizer.normalize("b")).thenReturn("db:b");
		when(pathNormalizer.normalize("c")).thenReturn("db:c");

		builder = new MapperBuilder(e, pathNormalizer, new KeyAdapterFactory());
	}

	@Test
	public void testCreateMapper_ByPropreties_Single() {
		Mapper mapper = builder.matchBy(new Property<Object>("a")).createMapper();
		assertNotNull(mapper);
		assertTrue(mapper instanceof PathMapper);
	}

	@Test
	public void testCreateMapper_ByPropreties_Multi() {
		Mapper mapper = builder.matchBy(new Property<Object>("a"), new Property<Object>("b")).createMapper();
		assertNotNull(mapper);
		assertTrue(mapper instanceof MultiPathMapper);
	}

	@Test
	public void testMatchBy_Additivity() {
		Map<String, Mapper> mappers = builder.matchBy(new Property<Object>("a"), new Property<Object>("b"))
				.matchBy("c").createPathMappers();
		assertEquals(3, mappers.size());
		assertTrue(mappers.containsKey("db:a"));
		assertTrue(mappers.containsKey("db:b"));
		assertTrue(mappers.containsKey("db:c"));
	}

	@Test
	public void testMatchBy_Duplicates() {
		Map<String, Mapper> mappers = builder.matchBy("a", "b", "b").matchBy("a").createPathMappers();
		assertEquals(2, mappers.size());
		assertTrue(mappers.containsKey("db:a"));
		assertTrue(mappers.containsKey("db:b"));
	}
}
