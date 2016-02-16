package com.nhl.link.move.itest.mapper;

import static org.junit.Assert.assertEquals;

import org.apache.cayenne.Cayenne;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.mapper.PathMapper;
import com.nhl.link.move.unit.DerbySrcTargetTest;
import com.nhl.link.move.unit.cayenne.t.Etl3t;

public class PathMapperIT extends DerbySrcTargetTest {

	private Etl3t e;

	@Before
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
