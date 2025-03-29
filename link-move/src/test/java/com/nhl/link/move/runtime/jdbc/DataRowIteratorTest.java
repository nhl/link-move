package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.RowAttribute;
import org.apache.cayenne.DataRow;
import org.apache.cayenne.ResultIterator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataRowIteratorTest {

    @Test
    public void calculateHeader() {

        DataRow row = new DataRow(5);
        row.put("A1", new Object());
        row.put("A2", null);
        row.put("A0", "aaaa");

        ResultIterator<DataRow> rows = mock(ResultIterator.class);
        when(rows.hasNextRow()).thenReturn(true, false);
        when(rows.nextRow()).thenReturn(row, (DataRow) null);

        DataRowIterator it = new DataRowIterator(rows);

        RowAttribute[] header = it.calculateHeader();
        assertNotNull(header);
        assertEquals(3, header.length);

        assertEquals("A0", header[0].getSourceName());
        assertEquals("db:A0", header[0].getTargetPath());

        assertEquals("A1", header[1].getSourceName());
        assertEquals("db:A1", header[1].getTargetPath());

        assertEquals("A2", header[2].getSourceName());
        assertEquals("db:A2", header[2].getTargetPath());
    }

    @Test
    public void calculateHeader_NoRows() {

        ResultIterator<DataRow> rows = mock(ResultIterator.class);
        when(rows.hasNextRow()).thenReturn(false, false);
        when(rows.nextRow()).thenReturn(null, (DataRow) null);

        DataRowIterator it = new DataRowIterator(rows);

        RowAttribute[] header = it.calculateHeader();
        assertNotNull(header);
        assertEquals(0, header.length);
    }
}
