package com.nhl.link.move.df;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class LazyDataFrameTest {

    private Index columns;
    private List<DataRow> rows;

    @Before
    public void initDataFrameParts() {
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

        DataFrame df = new LazyDataFrame(columns, rows).mapColumn("b", x -> x != null ? x.toString() : null);

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

        DataFrame df = new LazyDataFrame(columns, rows)
                .map(columns, (i, r) -> r
                        .mapColumn(0, (String v) -> v + "_")
                        .mapColumn(1, (Integer v) -> v != null ? v * 10 : null));

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

    @Test
    public void testMap_ChangeRowStructure() {

        Index i1 = new Index("c", "d", "e");

        DataFrame df = new LazyDataFrame(columns, rows)
                .map(i1, (i, r) -> new ArrayDataRow(
                        i,
                        r.get(0),
                        r.get(1) != null ? ((int) r.get(1)) * 10 : 0,
                        r.get(1)));

        assertSame(i1, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one", consumed.get(0).get("c"));
        assertEquals(10, consumed.get(0).get("d"));
        assertEquals(1, consumed.get(0).get("e"));

        assertEquals("two", consumed.get(1).get("c"));
        assertEquals(20, consumed.get(1).get("d"));
        assertEquals(2, consumed.get(1).get("e"));

        assertEquals("three", consumed.get(2).get("c"));
        assertEquals(30, consumed.get(2).get("d"));
        assertEquals(3, consumed.get(2).get("e"));

        assertEquals("four", consumed.get(3).get("c"));
        assertEquals(40, consumed.get(3).get("d"));
        assertEquals(4, consumed.get(3).get("e"));
    }

    @Test
    public void testMap_ChangeRowStructure_Chained() {

        Index i1 = new Index("c", "d", "e");
        Index i2 = new Index("f", "g");

        DataFrame df = new LazyDataFrame(columns, rows)
                .map(i1, (i, r) -> new ArrayDataRow(
                        i,
                        r.get(0),
                        r.get(1) != null ? ((int) r.get(1)) * 10 : 0,
                        r.get(1)))
                .map(i2, (i, r) -> new ArrayDataRow(
                        i,
                        r.get(0),
                        r.get(1)));

        assertSame(i2, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(4, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one", consumed.get(0).get("f"));
        assertEquals(10, consumed.get(0).get("g"));

        assertEquals("two", consumed.get(1).get("f"));
        assertEquals(20, consumed.get(1).get("g"));

        assertEquals("three", consumed.get(2).get("f"));
        assertEquals(30, consumed.get(2).get("g"));

        assertEquals("four", consumed.get(3).get("f"));
        assertEquals(40, consumed.get(3).get("g"));
    }

    @Test
    public void testMap_ChangeRowStructure_EmptyDF() {

        Index i1 = new Index("c", "d", "e");

        DataFrame df = new LazyDataFrame(columns)
                .map(i1, (i, r) -> new ArrayDataRow(
                        i,
                        r.get(0),
                        r.get(1) != null ? ((int) r.get(1)) * 10 : 0,
                        r.get(1)));

        assertSame(i1, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(0, consumed.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMap_Index_Row_SizeMismatch() {

        Index i1 = new Index("c", "d", "e");

        new LazyDataFrame(columns, rows)
                .map(i1, (i, r) -> new ArrayDataRow(
                        i,
                        r.get(0),
                        r.get(1)))
                // must throw when iterating due to inconsistent mapped row structure...
                .forEach(r -> {
                });
    }

    @Test
    public void testToString() {
        DataFrame df = new LazyDataFrame(columns, rows);
        assertEquals("LazyDataFrame [{a:one,b:1},{a:two,b:2},{a:three,b:3},...]", df.toString());
    }

    @Test
    public void testZip() {

        Index i1 = new Index("a");
        DataFrame df1 = new LazyDataFrame(i1, asList(
                new ArrayDataRow(i1, 1),
                new ArrayDataRow(i1, 2)));

        Index i2 = new Index("b");
        DataFrame df2 = new LazyDataFrame(i2, asList(
                new ArrayDataRow(i2, 10),
                new ArrayDataRow(i2, 20)));

        DataFrame df_zipped = df1.zip(df2);

        assertNotSame(df1, df_zipped);
        assertNotSame(df2, df_zipped);
        assertEquals(2, df_zipped.getColumns().size());

        assertArrayEquals(new String[]{"a", "b"}, df_zipped.getColumns().getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df_zipped.forEach(consumed::add);

        assertEquals(2, consumed.size());

        assertEquals(1, consumed.get(0).get("a"));
        assertEquals(10, consumed.get(0).get("b"));

        assertEquals(2, consumed.get(1).get("a"));
        assertEquals(20, consumed.get(1).get("b"));
    }

    @Test
    public void testZip_Self() {

        Index i1 = new Index("a");
        DataFrame df1 = new LazyDataFrame(i1, asList(
                new ArrayDataRow(i1, 1),
                new ArrayDataRow(i1, 2)));

        DataFrame df_zipped = df1.zip(df1);

        assertNotSame(df1, df_zipped);
        assertEquals(2, df_zipped.getColumns().size());

        assertArrayEquals(new String[]{"a", "a_"}, df_zipped.getColumns().getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df_zipped.forEach(consumed::add);

        assertEquals(2, consumed.size());

        assertEquals(1, consumed.get(0).get("a"));
        assertEquals(1, consumed.get(0).get("a_"));

        assertEquals(2, consumed.get(1).get("a"));
        assertEquals(2, consumed.get(1).get("a_"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZip_LeftSizeMismatch() {

        DataFrame df1 = new LazyDataFrame(columns, rows);
        DataFrame df2 = new LazyDataFrame(columns, asList(
                new ArrayDataRow(columns, "one", 1),
                new ArrayDataRow(columns, "two", 2),
                new ArrayDataRow(columns, "three", 3)));

        df1.zip(df2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZip_RightSizeMismatch() {

        DataFrame df1 = new LazyDataFrame(columns, rows);
        DataFrame df2 = new LazyDataFrame(columns, asList(
                new ArrayDataRow(columns, "one", 1),
                new ArrayDataRow(columns, "two", 2),
                new ArrayDataRow(columns, "three", 3)));

        df2.zip(df1);
    }
}
