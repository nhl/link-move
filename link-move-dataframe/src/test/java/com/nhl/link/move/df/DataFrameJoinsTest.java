package com.nhl.link.move.df;

import com.nhl.link.move.df.join.JoinSemantics;
import com.nhl.link.move.df.join.Joiner;
import org.junit.Test;

import java.util.Objects;

import static java.util.Arrays.asList;

public class DataFrameJoinsTest {

    @Test
    public void testJoin() {

        Index i1 = new Index("a", "b");
        DataFrame df1 = DataFrame.fromRows(i1, asList(
                new ArrayDataRow(i1, 1, "x"),
                new ArrayDataRow(i1, 2, "y")));

        Index i2 = new Index("c", "d");
        DataFrame df2 = DataFrame.fromRows(i2, asList(
                new ArrayDataRow(i2, 2, "a"),
                new ArrayDataRow(i2, 2, "b"),
                new ArrayDataRow(i2, 3, "c")));

        DataFrame df = df1.join(df2, (lr, rr) -> Objects.equals(lr.get(0), rr.get(0)));

        new DFAsserts(df, "a", "b", "c", "d")
                .assertLength(2)
                .assertRow(0, 2, "y", 2, "a")
                .assertRow(1, 2, "y", 2, "b");
    }

    @Test
    public void testJoin_NoMatches() {

        Index i1 = new Index("a", "b");
        DataFrame df1 = DataFrame.fromRows(i1, asList(
                new ArrayDataRow(i1, 1, "x"),
                new ArrayDataRow(i1, 2, "y")));

        Index i2 = new Index("c", "d");
        DataFrame df2 = DataFrame.fromRows(i2, asList(
                new ArrayDataRow(i2, 2, "a"),
                new ArrayDataRow(i2, 2, "b"),
                new ArrayDataRow(i2, 3, "c")));

        DataFrame df = df1.join(df2, (lr, rr) -> false);

        new DFAsserts(df, "a", "b", "c", "d")
                .assertLength(0);
    }

    @Test
    public void testLeftJoin() {

        Index i1 = new Index("a", "b");
        DataFrame df1 = DataFrame.fromRows(i1, asList(
                new ArrayDataRow(i1, 1, "x"),
                new ArrayDataRow(i1, 2, "y")));

        Index i2 = new Index("c", "d");
        DataFrame df2 = DataFrame.fromRows(i2, asList(
                new ArrayDataRow(i2, 2, "a"),
                new ArrayDataRow(i2, 2, "b"),
                new ArrayDataRow(i2, 3, "c")));

        Joiner joiner = new Joiner((lr, rr) -> Objects.equals(lr.get(0), rr.get(0)), JoinSemantics.left);
        DataFrame df = df1.join(df2, joiner);

        new DFAsserts(df, "a", "b", "c", "d")
                .assertLength(3)
                .assertRow(0, 1, "x", null, null)
                .assertRow(1, 2, "y", 2, "a")
                .assertRow(2, 2, "y", 2, "b");
    }

    @Test
    public void testRightJoin() {

        Index i1 = new Index("a", "b");
        DataFrame df1 = DataFrame.fromRows(i1, asList(
                new ArrayDataRow(i1, 1, "x"),
                new ArrayDataRow(i1, 2, "y")));

        Index i2 = new Index("c", "d");
        DataFrame df2 = DataFrame.fromRows(i2, asList(
                new ArrayDataRow(i2, 2, "a"),
                new ArrayDataRow(i2, 2, "b"),
                new ArrayDataRow(i2, 3, "c")));

        Joiner joiner = new Joiner((lr, rr) -> Objects.equals(lr.get(0), rr.get(0)), JoinSemantics.right);
        DataFrame df = df2.join(df1, joiner);

        new DFAsserts(df, "c", "d", "a", "b")
                .assertLength(3)
                .assertRow(0, null, null, 1, "x")
                .assertRow(1, 2, "a", 2, "y")
                .assertRow(2, 2, "b", 2, "y");
    }

    @Test
    public void testFullJoin() {

        Index i1 = new Index("a", "b");
        DataFrame df1 = DataFrame.fromRows(i1, asList(
                new ArrayDataRow(i1, 1, "x"),
                new ArrayDataRow(i1, 2, "y")));

        Index i2 = new Index("c", "d");
        DataFrame df2 = DataFrame.fromRows(i2, asList(
                new ArrayDataRow(i2, 2, "a"),
                new ArrayDataRow(i2, 2, "b"),
                new ArrayDataRow(i2, 3, "c")));

        Joiner joiner = new Joiner((lr, rr) -> Objects.equals(lr.get(0), rr.get(0)), JoinSemantics.full);
        DataFrame df = df1.join(df2, joiner);

        new DFAsserts(df, "a", "b", "c", "d")
                .assertLength(4)
                .assertRow(0, 1, "x", null, null)
                .assertRow(1, 2, "y", 2, "a")
                .assertRow(2, 2, "y", 2, "b")
                .assertRow(3, null, null, 3, "c");
    }
}
