package com.nhl.link.move.runtime.json;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.QueryCompiler;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JsonExtractorTest {

    private IJacksonService jacksonService;
    private QueryCompiler compiler;
    private StreamConnector connector;

    @Before
    public void setUp() throws Exception {

        jacksonService = new JacksonService();
        compiler = new QueryCompiler();
        String source =
                "{ \"store\": {\n" +
                "    \"book\": [ \n" +
                "      { \"category\": \"reference\",\n" +
                "        \"author\": \"Nigel Rees\",\n" +
                "        \"title\": \"Sayings of the Century\",\n" +
                "        \"price\": 8.95,\n" +
                "        \"readers\": [\n" +
                "          {\"name\": \"Bob\"},\n" +
                "          {\"name\": \"Rob\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      { \"category\": \"fiction\",\n" +
                "        \"author\": \"Evelyn Waugh\",\n" +
                "        \"title\": \"Sword of Honour\",\n" +
                "        \"price\": 12.99,\n" +
                "        \"readers\": [\n" +
                "          {\"name\": \"John\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      { \"category\": \"fiction\",\n" +
                "        \"author\": \"Herman Melville\",\n" +
                "        \"title\": \"Moby Dick\",\n" +
                "        \"isbn\": \"0-553-21311-3\",\n" +
                "        \"price\": 8.99\n" +
                "      },\n" +
                "      { \"category\": \"fiction\",\n" +
                "        \"author\": \"J. R. R. Tolkien\",\n" +
                "        \"title\": \"The Lord of the Rings\",\n" +
                "        \"isbn\": \"0-395-19395-8\",\n" +
                "        \"price\": 22.99\n" +
                "      }\n" +
                "    ],\n" +
                "    \"bicycle\": {\n" +
                "      \"color\": \"red\",\n" +
                "      \"price\": 19.95\n" +
                "    }\n" +
                "  }\n" +
                "}";

        connector = mock(StreamConnector.class);
        when(connector.getInputStream()).thenReturn(
                new ByteArrayInputStream(source.getBytes("UTF-8")));
    }

    @Test
    public void testJsonExtractor() {

        RowAttribute[] attributes = new RowAttribute[1];
        RowAttribute readerName = attributes[0] = new BaseRowAttribute(String.class, "name", "readerName", 0);

        JsonQuery query = compiler.compile("$.store.book[*].readers[*]");

        Extractor extractor = new JsonExtractor(jacksonService, connector, attributes, query);
        RowReader reader = extractor.getReader(new HashMap<String, Object>());
        Iterator<Row> iter = reader.iterator();
        List<Row> rows = new ArrayList<>();
        while (iter.hasNext()) {
            rows.add(iter.next());
        }

        List<String> readerNames = new ArrayList<>();
        for (Row row : rows) {
            readerNames.add((String) row.get(readerName));
        }
        assertEquals(3, readerNames.size());
        assertTrue(readerNames.containsAll(Arrays.asList("Bob", "John", "Rob")));
    }
}
