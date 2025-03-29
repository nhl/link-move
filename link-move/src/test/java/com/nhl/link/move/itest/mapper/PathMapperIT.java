package com.nhl.link.move.itest.mapper;

import com.nhl.link.move.mapper.PathMapper;
import com.nhl.link.move.unit.DerbySrcTargetTest;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import org.apache.cayenne.Cayenne;
import org.apache.cayenne.ObjectContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PathMapperIT extends DerbySrcTargetTest {

    Etl3t testObject;

    @BeforeEach
    void createTestObject() {
        ObjectContext context = targetCayenne.getRuntime().newContext();
        testObject = context.newObject(Etl3t.class);
        testObject.setPhoneNumber("123458");
        context.commitChanges();
    }

    @Test
    public void keyForTarget_Obj() {
        PathMapper mapper = new PathMapper("phoneNumber");
        assertEquals("123458", mapper.keyForTarget(testObject));
    }

    @Test
    public void keyForTarget_Db() {
        PathMapper mapper = new PathMapper("db:phone_number");
        assertEquals("123458", mapper.keyForTarget(testObject));
    }

    @Test
    public void keyForTarget_DbId() {
        PathMapper mapper = new PathMapper("db:id");
        assertEquals(Cayenne.intPKForObject(testObject), mapper.keyForTarget(testObject));
    }
}
