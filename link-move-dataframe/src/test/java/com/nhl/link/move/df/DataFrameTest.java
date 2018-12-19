package com.nhl.link.move.df;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class DataFrameTest {

    private DataFrame df;

    @Before
    public void initDataFrame() {
        Index columns = new Index("a");
        List<DataRow> rows = asList(
                new ArrayDataRow(columns, "one"),
                new ArrayDataRow(columns, "two"),
                new ArrayDataRow(columns, "three"),
                new ArrayDataRow(columns, "four"));


        this.df = new LazyDataFrame(columns, rows);
    }

    @Test
    public void testConsumeAsBatches_SizeNotDivisible() {

        int[] batchCounter = new int[1];
        int[] elementCounter = new int[1];

        df.consumeAsBatches(f -> {
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

        df.consumeAsBatches(f -> {
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

        df.consumeAsBatches(f -> {
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

        df.consumeAsBatches(f -> {
            batchCounter[0] += 1;
            f.forEach(r -> elementCounter[0] += 1);
        }, 1000);

        assertEquals(1, batchCounter[0]);
        assertEquals(4, elementCounter[0]);
    }
}
