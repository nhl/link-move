package com.nhl.link.move.df;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class LazyDataFrameTest {

    private Columns columns;
    private List<DataRow> rows;

    @Before
    public void initDataFrame() {
        this.columns = new Columns(new Column<>("a", String.class), new Column<>("b", Integer.class));
        this.rows = asList(
                new SimpleDataRow(columns, "one", 1),
                new SimpleDataRow(columns, "two", 2),
                new SimpleDataRow(columns, "three", 3),
                new SimpleDataRow(columns, "four", 4));
    }

    @Test
    public void testForEach() {

        List<DataRow> consumed = new ArrayList<>();

        new LazyDataFrame(columns, rows).forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertEquals(rows, consumed);
    }

    @Test
    public void testHead() {

        List<DataRow> consumed = new ArrayList<>();

        new LazyDataFrame(columns, rows).head(3).forEach(consumed::add);

        assertEquals(3, consumed.size());
        assertEquals(rows.subList(0, 3), consumed);
    }

    @Test
    public void testRenameColumn() {

        DataFrame df = new LazyDataFrame(columns, rows).renameColumn("b", "c");

        assertEquals(2, df.getColumns().size());
        assertNotSame(columns, df.getColumns());
        assertEquals("a", df.getColumns().getColumns()[0].getName());
        assertEquals("c", df.getColumns().getColumns()[1].getName());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one", consumed.get(0).get("a"));
        assertEquals(Integer.valueOf(1), consumed.get(0).get("c"));
    }

    @Test
    public void testConvertColumn() {

        DataFrame df = new LazyDataFrame(columns, rows).convertType("b", String.class, Object::toString);

        assertEquals(2, df.getColumns().size());
        assertNotSame(columns, df.getColumns());
        assertEquals("b", df.getColumns().getColumns()[1].getName());
        assertEquals(String.class, df.getColumns().getColumns()[1].getType());

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
}
