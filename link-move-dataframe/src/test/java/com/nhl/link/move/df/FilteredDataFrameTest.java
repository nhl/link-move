package com.nhl.link.move.df;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class FilteredDataFrameTest {

    @Test
    public void testIterator() {

        Index i = Index.withNames("a");
        List<Object[]> rows = asList(
                DataRow.row(1),
                DataRow.row(4));

        FilteredDataFrame df = new FilteredDataFrame(i, rows, r -> ((int) r[0]) > 2);

        new DFAsserts(df, "a")
                .assertLength(1)
                .assertRow(0, 4);
    }

    @Test
    public void testIterator_Empty() {

        Index i = Index.withNames("a");
        List<Object[]> rows = Collections.emptyList();

        FilteredDataFrame df = new FilteredDataFrame(i, rows, r -> ((int) r[0]) > 2);

        new DFAsserts(df, "a").assertLength(0);
    }

    @Test
    public void testIterator_NoMatch() {

        Index i = Index.withNames("a");
        List<Object[]> rows = asList(
                DataRow.row( 1),
                DataRow.row(4));

        FilteredDataFrame df = new FilteredDataFrame(i, rows, r -> ((int) r[0]) > 4);

        new DFAsserts(df, "a").assertLength(0);
    }

    @Test
    public void testMap() {

        Index i = Index.withNames("a");
        DataFrame df = new FilteredDataFrame(i, asList(
                DataRow.row("one"),
                DataRow.row("two")),
                r -> r[0].equals("two")).map(i, r -> i.mapColumn(r, "a", v -> v[0] + "_"));

        new DFAsserts(df, i)
                .assertLength(1)
                .assertRow(0, "two_");
    }

    @Test
    public void testRenameColumn() {
        Index i = Index.withNames("a", "b");

        DataFrame df = new FilteredDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2)), r -> true).renameColumn("b", "c");

        new DFAsserts(df, "a", "c")
                .assertLength(2)
                .assertRow(0, "one", 1)
                .assertRow(1, "two", 2);
    }
}
