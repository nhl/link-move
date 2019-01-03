package com.nhl.link.move.df;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DFAsserts {

    private String[] expectedColumns;
    private List<Object[]> rows;

    public DFAsserts(DataFrame df, Index expectedColumns) {
        this(df, expectedColumns.getNames());
    }

    public DFAsserts(DataFrame df, String... expectedIndex) {

        assertNotNull("DataFrame is null", df);
        assertArrayEquals("DataFrame columns differ from expected", expectedIndex, df.getColumns().getNames());

        this.expectedColumns = expectedIndex;
        this.rows = new ArrayList<>();
        df.forEach(rows::add);
    }

    public static void assertRow(Object[] row, String[] expectedIndex, Object... expectedValues) {

        assertEquals(expectedIndex.length, row.length);

        for (int i = 0; i < expectedIndex.length; i++) {
            assertEquals("Unexpected value for '" + expectedIndex[i] + " (" + i + ")'", expectedValues[i], row[i]);
        }
    }

    public DFAsserts assertLength(int expectedLength) {
        assertEquals("Unexpected DataFrame length", expectedLength, rows.size());
        return this;
    }

    public DFAsserts assertRow(int i, Object... expectedValues) {
        DFAsserts.assertRow(rows.get(i), expectedColumns, expectedValues);
        return this;
    }
}
