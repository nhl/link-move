package com.nhl.link.move.itest.mapper;

import com.nhl.link.move.mapper.PathMapper;
import com.nhl.link.move.unit.DerbySrcTargetTest;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import org.apache.cayenne.Cayenne;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PathMapperIT extends DerbySrcTargetTest {

	private Etl3t e;

	@BeforeEach
	public void before() {
		e = targetContext.newObject(Etl3t.class);
		e.setPhoneNumber("123458");
		targetContext.commitChanges();
	}

	@Test
	public void testKeyForTarget_Obj() {
		PathMapper mapper = new PathMapper("phoneNumber");
		assertEquals("123458", mapper.keyForTarget(e));
	}
	
	@Test
	public void testKeyForTarget_Db() {
		PathMapper mapper = new PathMapper("db:phone_number");
		assertEquals("123458", mapper.keyForTarget(e));
	}
	
	@Test
	public void testKeyForTarget_DbId() {
		PathMapper mapper = new PathMapper("db:id");
		assertEquals(Cayenne.intPKForObject(e), mapper.keyForTarget(e));
	}
}
