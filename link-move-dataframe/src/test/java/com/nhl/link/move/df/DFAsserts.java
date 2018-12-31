package com.nhl.link.move.df;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DFAsserts {

    private String[] expectedColumns;
    private List<DataRow> rows;

    public DFAsserts(DataFrame df, Index expectedColumns) {
        this(df, expectedColumns.getColumns());
    }

    public DFAsserts(DataFrame df, String... expectedIndex) {

        assertNotNull("DataFrame is null", df);
        assertArrayEquals("DataFrame columns differ from expected", expectedIndex, df.getColumns().getColumns());

        this.expectedColumns = expectedIndex;
        this.rows = new ArrayList<>();
        df.forEach(rows::add);
    }

    public static void assertRow(DataRow row, String[] expectedIndex, Object... expectedValues) {

        assertEquals(expectedIndex.length, row.size());
        assertArrayEquals(expectedIndex, row.getIndex().getColumns());

        for (int i = 0; i < expectedIndex.length; i++) {
            assertEquals("Unexpected value for '" + expectedIndex[i] + " (" + i + ")'", expectedValues[i], row.get(i));
        }
    }

    public DFAsserts assertLength(int expectedLength) {
        assertEquals(expectedLength, rows.size());
        return this;
    }

    public DFAsserts assertRow(int i, Object... expectedValues) {
        DFAsserts.assertRow(rows.get(i), expectedColumns, expectedValues);
        return this;
    }
}
