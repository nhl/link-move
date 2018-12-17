package com.nhl.link.move.df;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class HeadDataFrameTest {


    private Columns columns;
    private List<DataRow> rows;

    @Before
    public void initDataFrame() {
        this.columns = new Columns(new Column<>("a", String.class));
        this.rows = asList(
                new SimpleDataRow(columns, "one"),
                new SimpleDataRow(columns, "two"),
                new SimpleDataRow(columns, "three"),
                new SimpleDataRow(columns, "four"));
    }

    @Test
    public void testConstructor() {

        List<DataRow> consumed = new ArrayList<>();

        HeadDataFrame df = new HeadDataFrame(new SimpleDataFrame(columns, rows), 3);

        df.forEach(consumed::add);

        assertEquals(3, consumed.size());
        assertEquals(rows.subList(0, 3), consumed);
    }

    @Test
    public void testHead() {

        HeadDataFrame df = new HeadDataFrame(new SimpleDataFrame(columns, rows), 3);

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
}
