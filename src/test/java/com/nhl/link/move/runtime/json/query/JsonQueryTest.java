package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.nhl.link.move.runtime.json.JacksonService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonQueryTest {

    private QueryCompiler compiler;
    private JsonNode document;

    @Before
    public void setUp() throws IOException {

        compiler = new QueryCompiler();
        document = new JacksonService().parseJson(
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
                "}");
    }

    @Test
    public void testQuery_SimpleProperties1() {
        JsonQuery query = compiler.compile("$.store.book[*].author");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(4, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof TextNode);
        }
    }

    @Test
    public void testQuery_SimpleProperties_Brackets() {
        JsonQuery query = compiler.compile("$.store.book[*][\"author\"]");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(4, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof TextNode);
        }
    }

    @Test
    public void testQuery_SimpleProperties_Brackets2() {
        JsonQuery query = compiler.compile("$['store'][\"book\"][*]['author']");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(4, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof TextNode);
        }
    }

    @Test
    public void testQuery_SimpleProperties2() {
        JsonQuery query = compiler.compile("$.store..price");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(5, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof DoubleNode);
        }
    }

    @Test
    public void testQuery_ArrayProperties() {
        JsonQuery query = compiler.compile("$.store.book[*].readers");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(2, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof ArrayNode);
        }
    }

    @Test
    public void testQuery_ArrayProperties_Flattened() {
        JsonQuery query = compiler.compile("$.store.book[*].readers.*");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(3, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof ObjectNode);
        }
    }

    @Test
    public void testQuery_ArrayProperties_ByIndex() {
        JsonQuery query = compiler.compile("$.store.book[*].readers[1]");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(1, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof ObjectNode);
        }
    }

    @Test
    public void testQuery_ArrayProperties_ByIndex2() {
        JsonQuery query = compiler.compile("$.store.book[*].readers.1");
        List<JsonNode> nodes = query.execute(document);
        assertEquals(1, nodes.size());
        for (JsonNode node : nodes) {
            assertTrue(node instanceof ObjectNode);
        }
    }
}
