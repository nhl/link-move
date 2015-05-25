package com.nhl.link.etl.runtime.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.apache.cayenne.DataRow;
import org.apache.cayenne.ResultIterator;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.RowAttribute;

public class JdbcRowReader_DynamicAttributesTest {

	private JdbcRowReader reader;

	@Before
	public void before() {
		@SuppressWarnings("unchecked")
		ResultIterator<DataRow> rows = mock(ResultIterator.class);
		this.reader = new JdbcRowReader(null, rows);
	}

	@Test
	public void testAttributesFromDataRow() {

		DataRow row = new DataRow(5);
		row.put("A1", new Object());
		row.put("A2", null);
		row.put("A0", "aaaa");

		RowAttribute[] attributes = reader.attributesFromDataRow(row);
		assertNotNull(attributes);
		assertEquals(3, attributes.length);

		assertEquals("A0", attributes[0].sourceName());
		assertEquals("db:A0", attributes[0].targetName());
		
		assertEquals("A1", attributes[1].sourceName());
		assertEquals("db:A1", attributes[1].targetName());
		
		assertEquals("A2", attributes[2].sourceName());
		assertEquals("db:A2", attributes[2].targetName());
	}
}
