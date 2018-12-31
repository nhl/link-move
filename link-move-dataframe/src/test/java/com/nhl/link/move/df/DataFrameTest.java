package com.nhl.link.move.df;

import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class DataFrameTest {

    private DataFrame createDataFrame() {
        Index columns = new Index("a");
        List<DataRow> rows = asList(
                new ArrayDataRow(columns, "one"),
                new ArrayDataRow(columns, "two"),
                new ArrayDataRow(columns, "three"),
                new ArrayDataRow(columns, "four"));


        return new SimpleDataFrame(columns, rows);
    }

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

    @Test
    public void testConsumeAsBatches_SizeNotDivisible() {

        int[] batchCounter = new int[1];
        int[] elementCounter = new int[1];

        createDataFrame().consumeAsBatches(f -> {
            batchCounter[0] += 1;
            f.forEach(r -> elementCounter[0] += 1);
        }, 3);

        assertEquals(2, batchCounter[0]);
        assertEquals(4, elementCounter[0]);
    }

    @Test
    public void testConsumeAsBatches_SizeDivisible() {

        int[] batchCounter = new int[1];
        int[] elementCounter = new int[1];

        createDataFrame().consumeAsBatches(f -> {
            batchCounter[0] += 1;
            f.forEach(r -> elementCounter[0] += 1);
        }, 2);

        assertEquals(2, batchCounter[0]);
        assertEquals(4, elementCounter[0]);
    }

    @Test
    public void testConsumeAsBatches_SizeSame() {

        int[] batchCounter = new int[1];
        int[] elementCounter = new int[1];

        createDataFrame().consumeAsBatches(f -> {
            batchCounter[0] += 1;
            f.forEach(r -> elementCounter[0] += 1);
        }, 4);

        assertEquals(1, batchCounter[0]);
        assertEquals(4, elementCounter[0]);
    }

    @Test
    public void testConsumeAsBatches_SizeGreater() {

        int[] batchCounter = new int[1];
        int[] elementCounter = new int[1];

        createDataFrame().consumeAsBatches(f -> {
            batchCounter[0] += 1;
            f.forEach(r -> elementCounter[0] += 1);
        }, 1000);

        assertEquals(1, batchCounter[0]);
        assertEquals(4, elementCounter[0]);
    }
}
