package com.nhl.link.move.df;

import org.junit.Test;

import static java.util.Arrays.asList;

public class DataFrameTest {

    @Test
    public void testCreate() {

        Index i = new Index("a");
        DataFrame df = DataFrame.create(i, asList(
                new Object[]{1},
                new Object[]{2}));

        new DFAsserts(df, i)
                .assertLength(2)
                .assertRow(0, 1)
                .assertRow(1, 2);
    }
}
