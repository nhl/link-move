package com.nhl.link.move.df;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class ZippingDataFrameTest {

    @Test
    public void testConstructor() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                new ArrayDataRow(i1, 1),
                new ArrayDataRow(i1, 2)));

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                new ArrayDataRow(i2, 10),
                new ArrayDataRow(i2, 20)));

        List<DataRow> consumed = new ArrayList<>();
        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2);

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, 1, 10)
                .assertRow(1, 2, 20);
    }

    @Test
    public void testHead() {
        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                new ArrayDataRow(i1, 1),
                new ArrayDataRow(i1, 2)));

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                new ArrayDataRow(i2, 10),
                new ArrayDataRow(i2, 20)));

        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2).head(1);

        new DFAsserts(df, "a", "b")
                .assertLength(1)
                .assertRow(0, 1, 10);
    }

    @Test
    public void testRenameColumn() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                new ArrayDataRow(i1, 1),
                new ArrayDataRow(i1, 2)));

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                new ArrayDataRow(i2, 10),
                new ArrayDataRow(i2, 20)));

        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2).renameColumn("b", "c");

        new DFAsserts(df, "a", "c")
                .assertLength(2)
                .assertRow(0, 1, 10)
                .assertRow(1, 2, 20);
    }

    @Test
    public void testMap() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                new ArrayDataRow(i1, "one"),
                new ArrayDataRow(i1, "two")));

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                new ArrayDataRow(i2, 1),
                new ArrayDataRow(i2, 2)));

        Index zippedColumns = new Index("x", "y");

        DataFrame df = new ZippingDataFrame(zippedColumns, df1, df2).map((i, r) -> r
                .mapColumn(0, (String v) -> v + "_")
                .mapColumn(1, (Integer v) -> v * 10));

        new DFAsserts(df, zippedColumns)
                .assertLength(2)
                .assertRow(0, "one_", 10)
                .assertRow(1, "two_", 20);
    }

    @Test
    public void testMap_ChangeRowStructure() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                new ArrayDataRow(i1, "one"),
                new ArrayDataRow(i1, "two")));

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                new ArrayDataRow(i2, 1),
                new ArrayDataRow(i2, 2)));

        Index mappedColumns = new Index("x", "y", "z");
        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2)
                .map(mappedColumns, (i, r) -> new ArrayDataRow(
                        i,
                        r.get(0),
                        r.get(1) != null ? ((int) r.get(1)) * 10 : 0,
                        r.get(1)));

        new DFAsserts(df, mappedColumns)
                .assertLength(2)
                .assertRow(0, "one", 10, 1)
                .assertRow(1, "two", 20, 2);
    }
}
