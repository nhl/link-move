package com.nhl.link.move.df;

import org.junit.Test;

import static java.util.Arrays.asList;

public class DataFrameTest {

    @Test
    public void testCreate() {

        Index i = Index.withNames("a");
        DataFrame df = DataFrame.create(i, asList(
                new Object[]{1},
                new Object[]{2}));

        new DFAsserts(df, i)
                .assertLength(2)
                .assertRow(0, 1)
                .assertRow(1, 2);
    }

    @Test
    public void testAddColumn() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .addColumn("c", r -> ((int) r[0]) * 10);

        new DFAsserts(df, "a", "b", "c")
                .assertLength(2)
                .assertRow(0, 1, "x", 10)
                .assertRow(1, 2, "y", 20);
    }

    @Test
    public void testAddColumn_Sparse() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .selectColumns("a")
                .addColumn("c", r -> ((int) r[0]) * 10);

        new DFAsserts(df, "a", "c")
                .assertLength(2)
                .assertRow(0, 1, 10)
                .assertRow(1, 2, 20);
    }

    @Test
    public void testSelectColumns() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .selectColumns("b");

        new DFAsserts(df, new IndexPosition(1, "b"))
                .assertLength(2)
                .assertRow(0, "x")
                .assertRow(1, "y");
    }

    @Test
    public void testDropColumns1() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .dropColumns("a");

        new DFAsserts(df, new IndexPosition(1, "b"))
                .assertLength(2)
                .assertRow(0, "x")
                .assertRow(1, "y");
    }

    @Test
    public void testDropColumns2() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .dropColumns("b");

        new DFAsserts(df, new IndexPosition(0, "a"))
                .assertLength(2)
                .assertRow(0, 1)
                .assertRow(1, 2);
    }

    @Test
    public void testDropColumns3() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .dropColumns();

        new DFAsserts(df, new IndexPosition(0, "a"), new IndexPosition(1, "b"))
                .assertLength(2)
                .assertRow(0, 1, "x")
                .assertRow(1, 2, "y");
    }

    @Test
    public void testDropColumns4() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .dropColumns("no_such_column");

        new DFAsserts(df, new IndexPosition(0, "a"), new IndexPosition(1, "b"))
                .assertLength(2)
                .assertRow(0, 1, "x")
                .assertRow(1, 2, "y");
    }

    @Test
    public void testMapColumn() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .mapColumn("a", r -> ((int) r[0]) * 10);

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, 10, "x")
                .assertRow(1, 20, "y");
    }

    @Test
    public void testMapColumn_Sparse() {
        Index i1 = Index.withNames("a", "b");
        DataFrame df = DataFrame.create(i1, asList(
                DataRow.row(1, "x"),
                DataRow.row(2, "y")))
                .selectColumns("b")
                // TODO: dirty - we are using our knowledge of the internal array structure when referencing index [1].
                //  Need a user-friendly and more transparent way to decode a row.
                .mapColumn("b", r -> r[1] + "_");

        new DFAsserts(df, "b")
                .assertLength(2)
                .assertRow(0, "x_")
                .assertRow(1, "y_");
    }
}
