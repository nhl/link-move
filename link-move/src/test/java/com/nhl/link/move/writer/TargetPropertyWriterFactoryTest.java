package com.nhl.link.move.writer;

import org.apache.cayenne.map.ObjEntity;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class TargetPropertyWriterFactoryTest {

    @Test(expected = NullPointerException.class)
    public void testGetOrCreateWriter_Null() {
        TargetPropertyWriterFactory<C1> factory = new TargetPropertyWriterFactory<>(C1.class, mock(ObjEntity.class));
        factory.getOrCreateWriter("dummy", "dbDummy", () -> null);
    }

    @Test
    public void testGetOrCreateWriter_Default() {
        TargetPropertyWriter expected = (t, v) -> {};

        TargetPropertyWriterFactory<C1> factory = new TargetPropertyWriterFactory<>(C1.class, mock(ObjEntity.class));
        assertSame(expected, factory.getOrCreateWriter("dummy", "dbDummy", () -> expected));
        assertSame(expected, factory.getOrCreateWriter("dummy", "dbDummy", () -> expected));
    }

    @Test
    public void testGetOrCreateWriter_Setter() {
        TargetPropertyWriter defaultWriter = (t, v) -> {};

        TargetPropertyWriterFactory<C1> factory = new TargetPropertyWriterFactory<>(C1.class, mock(ObjEntity.class));
        TargetPropertyWriter setter = factory.getOrCreateWriter("me", "dbDummy", () -> defaultWriter);
        assertNotSame(defaultWriter, setter);
        assertSame(setter, factory.getOrCreateWriter("me", "dbDummy", () -> defaultWriter));
    }

    public static class C1 {

        public void setMe(String me) {

        }
    }
}
