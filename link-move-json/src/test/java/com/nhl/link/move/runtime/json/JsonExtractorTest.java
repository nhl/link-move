package com.nhl.link.move.runtime.json;

import com.nhl.link.move.BaseRowAttribute;
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
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyMap;
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
                        "          {\"name\": \"Bob\", \"details\": {\"age\":18}},\n" +
                        "          {\"name\": \"Rob\", \"details\": {\"age\":60}}\n" +
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
        when(connector.getInputStream(anyMap())).thenReturn(
                new ByteArrayInputStream(source.getBytes("UTF-8")));
    }

    @Test
    public void testJsonExtractor_SimpleAttributes() {

        RowAttribute baseAttr = new BaseRowAttribute(String.class, "title", "title", 0);
        JsonRowAttribute[] attributes = new JsonRowAttribute[]{
                new JsonRowAttribute(baseAttr, compiler)
        };

        JsonQuery query = compiler.compile("$.store.book[*]");

        List<Object[]> rows = collectRows(attributes, query);

        List<String> items = new ArrayList<>();
        for (Object[] row : rows) {
            items.add((String) row[0]);
        }
        assertEquals(4, items.size());
        assertTrue(items.containsAll(Arrays.asList("Sayings of the Century", "Sword of Honour", "Moby Dick",
                "The Lord of the Rings")));
    }

    @Test
    public void testJsonExtractor_QueryAttributes_Local() {

        RowAttribute baseAttr = new BaseRowAttribute(String.class, "@.details.age", "age", 0);
        JsonRowAttribute[] attributes = new JsonRowAttribute[]{
                new JsonRowAttribute(baseAttr, compiler)
        };

        JsonQuery query = compiler.compile("$.store.book[*].readers[*]");

        List<Object[]> rows = collectRows(attributes, query);

        List<Object> items = new ArrayList<>();
        for (Object[] row : rows) {
            items.add(row[0]);
        }
        assertEquals(3, items.size());
        assertTrue(items.containsAll(Arrays.asList(18, 60, null)));
    }

    @Test
    public void testJsonExtractor_QueryAttributes_Root() {

        RowAttribute baseAttr = new BaseRowAttribute(String.class, "$.store.bicycle.color", "constantAttr", 0);
        JsonRowAttribute[] attributes = new JsonRowAttribute[]{
                new JsonRowAttribute(baseAttr, compiler)
        };

        JsonQuery query = compiler.compile("$.store.book[*].readers[*]");

        List<Object[]> rows = collectRows(attributes, query);

        List<String> items = new ArrayList<>();
        for (Object[] row : rows) {
            items.add((String) row[0]);
        }

        assertEquals(Arrays.asList("red", "red", "red"), items);
    }

    @Test
    public void testJsonExtractor_QueryAttributes_Parent() {

        RowAttribute baseAttr = new BaseRowAttribute(String.class, "@#parent#parent.bicycle.color", "constantAttr", 0);
        JsonRowAttribute[] attributes = new JsonRowAttribute[]{
                new JsonRowAttribute(baseAttr, compiler)
        };

        JsonQuery query = compiler.compile("$.store.book[*]");

        List<Object[]> rows = collectRows(attributes, query);

        List<String> items = new ArrayList<>();
        for (Object[] row : rows) {
            items.add((String) row[0]);
        }

        assertEquals(Arrays.asList("red", "red", "red", "red"), items);
    }

    private List<Object[]> collectRows(JsonRowAttribute[] attributes, JsonQuery query) {

        Extractor extractor = new JsonExtractor(jacksonService, connector, attributes, query);
        RowReader reader = extractor.getReader(new HashMap<>());

        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : reader) {
            rows.add(row);
        }
        return rows;
    }
}
