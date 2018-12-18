package com.nhl.link.move.df;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class EagerDataFrameTest {

    private Index columns;
    private List<DataRow> rows;

    @Before
    public void initDataFrame() {
        this.columns = new Index("a", "b");
        this.rows = asList(
                new ArrayDataRow(columns, "one", 1),
                new ArrayDataRow(columns, "two", 2),
                new ArrayDataRow(columns, "three", 3),
                new ArrayDataRow(columns, "four", 4));
    }

    @Test
    public void testForEach() {

        List<DataRow> consumed = new ArrayList<>();

        new EagerDataFrame(columns, rows).forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertEquals(rows, consumed);
    }

    @Test
    public void testHead() {

        List<DataRow> consumed = new ArrayList<>();

        new EagerDataFrame(columns, rows).head(3).forEach(consumed::add);

        assertEquals(3, consumed.size());
        assertEquals(rows.subList(0, 3), consumed);
    }

    @Test
    public void testRenameColumn() {

        DataFrame df = new EagerDataFrame(columns, rows).renameColumn("b", "c");

        assertEquals(2, df.getColumns().size());
        assertNotSame(columns, df.getColumns());
        assertEquals("a", df.getColumns().getColumns()[0]);
        assertEquals("c", df.getColumns().getColumns()[1]);

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one", consumed.get(0).get("a"));
        assertEquals(Integer.valueOf(1), consumed.get(0).get("c"));
    }

    @Test
    public void testMapColumn() {

        DataFrame df = new EagerDataFrame(columns, rows).mapColumn("b", Object::toString);

        assertEquals(2, df.getColumns().size());
        assertSame(columns, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one", consumed.get(0).get("a"));
        assertEquals("1", consumed.get(0).get("b"));

        assertEquals("2", consumed.get(1).get("b"));
        assertEquals("3", consumed.get(2).get("b"));
        assertEquals("4", consumed.get(3).get("b"));
    }

    @Test
    public void testMap() {

        DataFrame df = new EagerDataFrame(columns, rows)
                .map(r -> r
                        .mapColumn(0, (String v) -> v + "_")
                        .mapColumn(1, (Integer i) -> i * 10));

        assertEquals(2, df.getColumns().size());
        assertSame(columns, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one_", consumed.get(0).get("a"));
        assertEquals(10, consumed.get(0).get("b"));

        assertEquals("two_", consumed.get(1).get("a"));
        assertEquals(20, consumed.get(1).get("b"));

        assertEquals("three_", consumed.get(2).get("a"));
        assertEquals(30, consumed.get(2).get("b"));

        assertEquals("four_", consumed.get(3).get("a"));
        assertEquals(40, consumed.get(3).get("b"));
    }
}
