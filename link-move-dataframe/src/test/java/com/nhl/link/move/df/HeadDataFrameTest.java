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

        HeadDataFrame df = new HeadDataFrame(new LazyDataFrame(columns, rows), 3);

        df.forEach(consumed::add);

        assertEquals(3, consumed.size());
        assertEquals(rows.subList(0, 3), consumed);
    }

    @Test
    public void testHead() {

        HeadDataFrame df = new HeadDataFrame(new LazyDataFrame(columns, rows), 3);

        DataFrame df1 = df.head(3);
        assertSame(df, df1);

        DataFrame df2 = df.head(5);
        assertSame(df, df2);

        DataFrame df3 = df.head(2);
        assertNotSame(df, df3);

        List<DataRow> consumed = new ArrayList<>();
        df3.forEach(consumed::add);

        assertEquals(2, consumed.size());
        assertEquals(rows.subList(0, 2), consumed);
    }

    @Test
    public void testMap() {

        DataFrame df =  new HeadDataFrame(new LazyDataFrame(columns, rows), 3)
                .map(columns, (i, r) -> r.mapColumn(0, (String v) -> v + "_"));

        assertEquals(1, df.getColumns().size());
        assertSame(columns, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(3, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one_", consumed.get(0).get("a"));
        assertEquals("two_", consumed.get(1).get("a"));
        assertEquals("three_", consumed.get(2).get("a"));
    }

    @Test
    public void testMap_ChangeRowStructure() {

        Index i1 = new Index("c");

        DataFrame df = new HeadDataFrame(new LazyDataFrame(columns, rows), 2)
                .map(i1, (i, r) -> new ArrayDataRow(i, r.get(0) != null ? r.get(0) + "_" : ""));

        assertSame(i1, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(2, consumed.size());
        assertNotEquals(rows, consumed);

        assertEquals("one_", consumed.get(0).get("c"));
        assertEquals("two_", consumed.get(1).get("c"));
    }

    @Test
    public void testMap_ChangeRowStructure_EmptyDF() {

        Index i1 = new Index("c");

        DataFrame df = new HeadDataFrame(new LazyDataFrame(columns, Collections.emptyList()), 2)
                .map(i1, (i, r) -> new ArrayDataRow(i, r.get(0) != null ? r.get(0) + "_" : ""));

        assertSame(i1, df.getColumns());

        List<DataRow> consumed = new ArrayList<>();
        df.forEach(consumed::add);

        assertEquals(0, consumed.size());
    }
}
