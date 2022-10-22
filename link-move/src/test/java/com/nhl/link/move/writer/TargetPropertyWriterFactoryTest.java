package com.nhl.link.move.writer;

import org.apache.cayenne.map.ObjEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class TargetPropertyWriterFactoryTest {

    @Test
    public void testGetOrCreateWriter_Null() {
        TargetPropertyWriterFactory factory = new TargetPropertyWriterFactory(C1.class, mock(ObjEntity.class));
        assertThrows(NullPointerException.class, () -> factory.getOrCreateWriter("dummy", "dbDummy", () -> null));
    }

    @Test
    public void testGetOrCreateWriter_Default() {
        TargetPropertyWriter expected = (t, v) -> {};

        TargetPropertyWriterFactory factory = new TargetPropertyWriterFactory(C1.class, mock(ObjEntity.class));
        assertSame(expected, factory.getOrCreateWriter("dummy", "dbDummy", () -> expected));
        assertSame(expected, factory.getOrCreateWriter("dummy", "dbDummy", () -> expected));
    }

    @Test
    public void testGetOrCreateWriter_Setter() {
        TargetPropertyWriter defaultWriter = (t, v) -> {};

        TargetPropertyWriterFactory factory = new TargetPropertyWriterFactory(C1.class, mock(ObjEntity.class));
        TargetPropertyWriter setter = factory.getOrCreateWriter("me", "dbDummy", () -> defaultWriter);
        assertNotSame(defaultWriter, setter);
        assertSame(setter, factory.getOrCreateWriter("me", "dbDummy", () -> defaultWriter));
    }

    public static class C1 {

        public void setMe(String me) {

        }
    }
}
