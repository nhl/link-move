package com.nhl.link.move.df;

import com.nhl.link.move.df.zip.Zipper;
import org.junit.Test;

import static java.util.Arrays.asList;

public class ZippingDataFrameTest {

    @Test
    public void testConstructor() {

        Index i1 = new Index("a");
        DataFrame df1 = new SimpleDataFrame(i1, asList(
                DataRow.row(1),
                DataRow.row(2)));

        Index i2 = new Index("b");
        DataFrame df2 = new SimpleDataFrame(i2, asList(
                DataRow.row(10),
                DataRow.row(20)));

        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2, Zipper.rowZipper(2));

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, 1, 10)
                .assertRow(1, 2, 20);
    }

    @Test
    public void testHead() {
        Index i1 = new Index("a");
        DataFrame df1 = new SimpleDataFrame(i1, asList(
                DataRow.row(1),
                DataRow.row(2)));

        Index i2 = new Index("b");
        DataFrame df2 = new SimpleDataFrame(i2, asList(
                DataRow.row(10),
                DataRow.row(20)));

        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2, Zipper.rowZipper(2))
                .head(1);

        new DFAsserts(df, "a", "b")
                .assertLength(1)
                .assertRow(0, 1, 10);
    }

    @Test
    public void testRenameColumn() {

        Index i1 = new Index("a");
        DataFrame df1 = new SimpleDataFrame(i1, asList(
                DataRow.row(1),
                DataRow.row(2)));

        Index i2 = new Index("b");
        DataFrame df2 = new SimpleDataFrame(i2, asList(
                DataRow.row(10),
                DataRow.row(20)));

        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2, Zipper.rowZipper(2))
                .renameColumn("b", "c");

        new DFAsserts(df, "a", "c")
                .assertLength(2)
                .assertRow(0, 1, 10)
                .assertRow(1, 2, 20);
    }

    @Test
    public void testMap() {

        Index i1 = new Index("a");
        DataFrame df1 = new SimpleDataFrame(i1, asList(
                DataRow.row("one"),
                DataRow.row("two")));

        Index i2 = new Index("b");
        DataFrame df2 = new SimpleDataFrame(i2, asList(
                DataRow.row(1),
                DataRow.row(2)));

        Index zippedColumns = new Index("x", "y");

        DataFrame df = new ZippingDataFrame(zippedColumns, df1, df2, Zipper.rowZipper(2))
                .map(r -> DataRow.mapColumn(r, 0, v -> v[0] + "_"));

        new DFAsserts(df, zippedColumns)
                .assertLength(2)
                .assertRow(0, "one_", 1)
                .assertRow(1, "two_", 2);
    }

    @Test
    public void testMap_ChangeRowStructure() {

        Index i1 = new Index("a");
        DataFrame df1 = new SimpleDataFrame(i1, asList(
                DataRow.row("one"),
                DataRow.row("two")));

        Index i2 = new Index("b");
        DataFrame df2 = new SimpleDataFrame(i2, asList(
                DataRow.row(1),
                DataRow.row(2)));

        Index mappedColumns = new Index("x", "y", "z");
        DataFrame df = new ZippingDataFrame(new Index("a", "b"), df1, df2, Zipper.rowZipper(2))
                .map(mappedColumns, r -> DataRow.row(
                        r[0],
                        ((int) r[1]) * 10,
                        r[1]));

        new DFAsserts(df, mappedColumns)
                .assertLength(2)
                .assertRow(0, "one", 10, 1)
                .assertRow(1, "two", 20, 2);
    }
}
