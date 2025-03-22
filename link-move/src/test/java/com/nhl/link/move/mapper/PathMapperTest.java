package com.nhl.link.move.mapper;

import org.dflib.Index;
import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.DataObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PathMapperTest {

    private PathMapper mapper;

    @BeforeEach
    public void before() {
        mapper = new PathMapper("abc");
    }

    @Test
    public void testKeyForSource() {
        assertEquals("ABC", mapper.keyForSource(new TestRowProxy(Index.of("a", "abc"), "A", "ABC")));
    }

    @Test
    public void testKeyForSource_NullKey() {
        assertEquals(null, mapper.keyForSource(new TestRowProxy(Index.of("a", "abc"), "A", null)));
    }

    @Test
    public void testKeyForSource_MissingKey() {
        assertThrows(LmRuntimeException.class,
                () -> mapper.keyForSource(new TestRowProxy(Index.of("a"), "A")));
    }

    @Test
    public void testKeyForTarget() {

        DataObject t = mock(DataObject.class);
        when(t.readProperty("abc")).thenReturn(44);
        when(t.readNestedProperty("abc")).thenReturn(44);

        assertEquals(44, mapper.keyForTarget(t));
    }

    @Test
    public void testExpressionForKey() {
        assertEquals("abc = \"a\"", mapper.expressionForKey("a").toString());
    }
}
