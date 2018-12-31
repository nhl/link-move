package com.nhl.link.move.df;

import org.junit.Test;

import static java.util.Arrays.asList;

public class DataFrameTest {

    @Test
    public void testFromRows() {

        Index i = new Index("a");
        DataFrame df = DataFrame.fromRows(i, asList(
                new ArrayDataRow(i, 1),
                new ArrayDataRow(i, 2)));

        new DFAsserts(df, i)
                .assertLength(2)
                .assertRow(0, 1)
                .assertRow(1, 2);
    }

    @Test
    public void testFromArrays() {

        Index i = new Index("a");
        DataFrame df = DataFrame.fromArrays(i, asList(
                new Object[]{1},
                new Object[]{2}));

        new DFAsserts(df, i)
                .assertLength(2)
                .assertRow(0, 1)
                .assertRow(1, 2);
    }
}
