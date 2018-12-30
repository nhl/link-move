package com.nhl.link.move.df;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class LazyDataFrameTest {

    @Test
    public void testForEach() {

        Index i = new Index("a", "b");
        List<DataRow> rows = asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2));
        List<DataRow> consumed = new ArrayList<>();

        new LazyDataFrame(i, rows).forEach(consumed::add);

        assertEquals(rows, consumed);
    }

    @Test
    public void testHead() {

        Index columns = new Index("a", "b");

        DataFrame df = new LazyDataFrame(columns, asList(
                new ArrayDataRow(columns, "one", 1),
                new ArrayDataRow(columns, "two", 2),
                new ArrayDataRow(columns, "three", 3))).head(2);

        new DFAsserts(df, columns)
                .assertLength(2)
                .assertRow(0, "one", 1)
                .assertRow(1, "two", 2);
    }

    @Test
    public void testRenameColumn() {
        Index i = new Index("a", "b");

        DataFrame df = new LazyDataFrame(i, asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2))).renameColumn("b", "c");

        new DFAsserts(df, "a", "c")
                .assertLength(2)
                .assertRow(0, "one", 1)
                .assertRow(1, "two", 2);
    }

    @Test
    public void testMapColumn() {

        Index i = new Index("a", "b");

        DataFrame df = new LazyDataFrame(i, asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2))).mapColumn("b", Object::toString);

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, "one", "1")
                .assertRow(1, "two", "2");
    }

    @Test
    public void testMap() {

        Index i = new Index("a", "b");

        DataFrame df = new LazyDataFrame(i, asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2)))
                .map(i, (ix, r) -> r.mapColumn(0, (String v) -> v + "_").mapColumn(1, (Integer v) -> v * 10));

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, "one_", 10)
                .assertRow(1, "two_", 20);
    }

    @Test
    public void testMap_ChangeRowStructure() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");

        DataFrame df = new LazyDataFrame(i, asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2)))
                .map(i1, (ix, r) -> new ArrayDataRow(
                        ix,
                        r.get(0),
                        ((int) r.get(1)) * 10,
                        r.get(1)));

        new DFAsserts(df, i1)
                .assertLength(2)
                .assertRow(0, "one", 10, 1)
                .assertRow(1, "two", 20, 2);
    }

    @Test
    public void testMap_ChangeRowStructure_Chained() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");
        Index i2 = new Index("f", "g");

        DataFrame df = new LazyDataFrame(i, asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2)))
                .map(i1, (ix, r) -> new ArrayDataRow(
                        ix,
                        r.get(0),
                        ((int) r.get(1)) * 10,
                        r.get(1)))
                .map(i2, (ix, r) -> new ArrayDataRow(
                        ix,
                        r.get(0),
                        r.get(1)));

        new DFAsserts(df, i2)
                .assertLength(2)
                .assertRow(0, "one", 10)
                .assertRow(1, "two", 20);
    }

    @Test
    public void testMap_ChangeRowStructure_EmptyDF() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");

        DataFrame df = new LazyDataFrame(i)
                .map(i1, (ix, r) -> new ArrayDataRow(
                        ix,
                        r.get(0),
                        ((int) r.get(1)) * 10,
                        r.get(1)));

        assertSame(i1, df.getColumns());

        new DFAsserts(df, i1).assertLength(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMap_Index_Row_SizeMismatch() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");

        new LazyDataFrame(i, asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2)))
                .map(i1, (ix, r) -> new ArrayDataRow(
                        ix,
                        r.get(0),
                        r.get(1)))
                // must throw when iterating due to inconsistent mapped row structure...
                .forEach(r -> {
                });
    }

    @Test
    public void testToString() {
        Index i = new Index("a", "b");
        DataFrame df = new LazyDataFrame(i, asList(
                new ArrayDataRow(i, "one", 1),
                new ArrayDataRow(i, "two", 2),
                new ArrayDataRow(i, "three", 3),
                new ArrayDataRow(i, "four", 4)));

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

        DataFrame df = df1.zip(df2);
        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, 1, 10)
                .assertRow(1, 2, 20);
    }

    @Test
    public void testZip_Self() {

        Index i1 = new Index("a");
        DataFrame df1 = new LazyDataFrame(i1, asList(
                new ArrayDataRow(i1, 1),
                new ArrayDataRow(i1, 2)));

        DataFrame df = df1.zip(df1);

        new DFAsserts(df, "a", "a_")
                .assertLength(2)
                .assertRow(0, 1, 1)
                .assertRow(1, 2, 2);
    }

    @Test
    public void testZip_LeftIsShorter() {

        Index i1 = new Index("a");
        DataFrame df1 = new LazyDataFrame(i1, asList(
                new ArrayDataRow(i1, 2)));

        Index i2 = new Index("b");
        DataFrame df2 = new LazyDataFrame(i2, asList(
                new ArrayDataRow(i2, 10),
                new ArrayDataRow(i2, 20)));

        DataFrame df = df1.zip(df2);
        new DFAsserts(df, "a", "b")
                .assertLength(1)
                .assertRow(0, 2, 10);
    }

    @Test
    public void testZip_RightIsShorter() {

        Index i1 = new Index("a");
        DataFrame df1 = new LazyDataFrame(i1, asList(
                new ArrayDataRow(i1, 2)));

        Index i2 = new Index("b");
        DataFrame df2 = new LazyDataFrame(i2, asList(
                new ArrayDataRow(i2, 10),
                new ArrayDataRow(i2, 20)));

        DataFrame df = df2.zip(df1);
        new DFAsserts(df, "b", "a")
                .assertLength(1)
                .assertRow(0, 10, 2);
    }
}
