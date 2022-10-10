package com.nhl.link.move.runtime.task;

import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.mapper.MultiPathMapper;
import com.nhl.link.move.mapper.PathMapper;
import com.nhl.link.move.runtime.key.KeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import org.apache.cayenne.exp.property.PropertyFactory;
import org.apache.cayenne.map.ObjEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapperBuilderTest {

	private MapperBuilder builder;

	private Optional<TargetAttribute> createAttribute(TargetEntity entity, String baseName) {
		return Optional.of(new TargetAttribute(entity, "db:" + baseName, 1, "java.lang.Object", Optional.empty()));
	}

	@BeforeEach
	public void before() {

		ObjEntity e = mock(ObjEntity.class);
		TargetEntity targetEntity = mock(TargetEntity.class);
		when(targetEntity.getAttribute("a")).thenReturn(createAttribute(targetEntity, "a"));
		when(targetEntity.getAttribute("b")).thenReturn(createAttribute(targetEntity, "b"));
		when(targetEntity.getAttribute("c")).thenReturn(createAttribute(targetEntity, "c"));

		builder = new MapperBuilder(e, targetEntity, new KeyAdapterFactory());
	}

	@Test
	public void testCreateMapper_ByPropreties_Single() {
		Mapper mapper = builder.matchBy(PropertyFactory.createString("a", String.class)).createMapper();
		assertNotNull(mapper);
		assertTrue(mapper instanceof PathMapper);
	}

	@Test
	public void testCreateMapper_ByPropreties_Multi() {
		Mapper mapper = builder.matchBy(PropertyFactory.createString("a", String.class), PropertyFactory.createString("b", String.class)).createMapper();
		assertNotNull(mapper);
		assertTrue(mapper instanceof MultiPathMapper);
	}

	@Test
	public void testMatchBy_Additivity() {
		Map<String, Mapper> mappers = builder.matchBy(PropertyFactory.createString("a", String.class), PropertyFactory.createString("b", String.class))
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
