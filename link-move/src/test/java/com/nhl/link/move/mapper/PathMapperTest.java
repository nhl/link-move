package com.nhl.link.move.mapper;

import com.nhl.dflib.row.ArrayRowProxy;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.dflib.Index;
import org.apache.cayenne.DataObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PathMapperTest {

    private PathMapper mapper;

    @Before
    public void before() {
        mapper = new PathMapper("abc");
    }

    @Test
    public void testKeyForSource() {
        assertEquals("ABC", mapper.keyForSource(new ArrayRowProxy(Index.withNames("a", "abc")).reset("A", "ABC")));
    }

    @Test
    public void testKeyForSource_NullKey() {
        assertEquals(null, mapper.keyForSource(new ArrayRowProxy(Index.withNames("a", "abc")).reset("A", null)));
    }

    @Test(expected = LmRuntimeException.class)
    public void testKeyForSource_MissingKey() {
        mapper.keyForSource(new ArrayRowProxy(Index.withNames("a")).reset("A"));
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
