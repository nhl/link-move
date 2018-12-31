package com.nhl.link.move.df;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class HeadDataFrameTest {

    private Index columns;
    private List<DataRow> rows;

    @Before
    public void initDataFrame() {
        this.columns = new Index("a");
        this.rows = asList(
                new ArrayDataRow(columns, "one"),
                new ArrayDataRow(columns, "two"),
                new ArrayDataRow(columns, "three"),
                new ArrayDataRow(columns, "four"));
    }

    @Test
    public void testConstructor() {

        List<DataRow> consumed = new ArrayList<>();

        HeadDataFrame df = new HeadDataFrame(new TransformingDataFrame(columns, rows), 3);

        df.forEach(consumed::add);

        assertEquals(rows.subList(0, 3), consumed);
    }

    @Test
    public void testHead() {

        HeadDataFrame df = new HeadDataFrame(new TransformingDataFrame(columns, rows), 3);

        DataFrame df1 = df.head(3);
        assertSame(df, df1);

        DataFrame df2 = df.head(5);
        assertSame(df, df2);

        DataFrame df3 = df.head(2);
        assertNotSame(df, df3);

        new DFAsserts(df3, columns)
                .assertLength(2)
                .assertRow(0, "one")
                .assertRow(1, "two");
    }

    @Test
    public void testMap() {

        DataFrame df = new HeadDataFrame(new TransformingDataFrame(columns, rows), 3)
                .map(columns, (i, r) -> r.mapColumn(0, v -> v + "_"));

        new DFAsserts(df, columns)
                .assertLength(3)
                .assertRow(0, "one_")
                .assertRow(1, "two_")
                .assertRow(2, "three_");
    }

    @Test
    public void testMap_ChangeRowStructure() {

        Index i1 = new Index("c");

        DataFrame df = new HeadDataFrame(new TransformingDataFrame(columns, rows), 2)
                .map(i1, (i, r) -> new ArrayDataRow(i, r.get(0) + "_"));

        new DFAsserts(df, i1)
                .assertLength(2)
                .assertRow(0, "one_")
                .assertRow(1, "two_");
    }

    @Test
    public void testMap_ChangeRowStructure_EmptyDF() {

        Index i1 = new Index("c");

        DataFrame df = new HeadDataFrame(new TransformingDataFrame(columns, Collections.emptyList()), 2)
                .map(i1, (i, r) -> new ArrayDataRow(i, r.get(0) + "_"));

        new DFAsserts(df, i1).assertLength(0);
    }

    @Test
    public void testZip_LeftIsShorter() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                new ArrayDataRow(i1, 1),
                new ArrayDataRow(i1, 2)));

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                new ArrayDataRow(i2, 10),
                new ArrayDataRow(i2, 20)));


        DataFrame df = new HeadDataFrame(df1, 1).zip(df2);

        new DFAsserts(df, "a", "b")
                .assertLength(1)
                .assertRow(0, 1, 10);
    }
}
