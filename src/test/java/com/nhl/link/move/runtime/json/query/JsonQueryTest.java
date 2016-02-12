package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nhl.link.move.runtime.json.JacksonService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonQueryTest {

    private JsonNodeFactory nodeFactory;
    private QueryCompiler compiler;
    private JsonNode document;

    @Before
    public void setUp() throws IOException {

        nodeFactory = JsonNodeFactory.instance;
        compiler = new QueryCompiler();
        document = new JacksonService().parseJson(
                "{ \"store\": {\n" +
                "    \"book\": [\n" +
                "      { \"category\": \"reference\",\n" +
                "        \"author\": \"Nigel Rees\",\n" +
                "        \"title\": \"Sayings of the Century\",\n" +
                "        \"price\": 8.95,\n" +
                "        \"readers\": [\n" +
                "          {\"name\": \"Bob\", \"age\": 18},\n" +
                "          {\"name\": \"Rob\", \"age\": 60}\n" +
                "        ]\n" +
                "      },\n" +
                "      { \"category\": \"fiction\",\n" +
                "        \"author\": \"Evelyn Waugh\",\n" +
                "        \"title\": \"Sword of Honour\",\n" +
                "        \"price\": 12.99,\n" +
                "        \"readers\": [\n" +
                "          {\"name\": \"John\", \"age\": 3}\n" +
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
                "  },\n" +
                "  \"allReaders\": [\n" +
                "    {\"name\": \"Bob\", \"age\": 18},\n" +
                "    {\"name\": \"Rob\", \"age\": 60},\n" +
                "    {\"name\": \"John\", \"age\": 3}\n" +
                "  ],\n" +
                "  \"indices\": [\n" +
                "    0, 1, 2\n" +
                "  ],\n" +
                "  \"properties\": [\n" +
                "    \"readers\"\n" +
                "  ]\n" +
                "}\n");
    }

    @Test
    public void testQuery_SimpleProperties_Dot() {

        JsonQuery query = compiler.compile("$.store.book[*].author");
        List<JsonNode> nodes = query.execute(document);

        assertEquals(4, nodes.size());

        assertTrue(nodes.contains(nodeFactory.textNode("Nigel Rees")));
        assertTrue(nodes.contains(nodeFactory.textNode("Evelyn Waugh")));
        assertTrue(nodes.contains(nodeFactory.textNode("Herman Melville")));
        assertTrue(nodes.contains(nodeFactory.textNode("J. R. R. Tolkien")));
    }

    @Test
    public void testQuery_SimpleProperties_Brackets() {

        JsonQuery query = compiler.compile("$['store'][\"book\"][*]['author']");
        List<JsonNode> nodes = query.execute(document);

        assertEquals(4, nodes.size());

        assertTrue(nodes.contains(nodeFactory.textNode("Nigel Rees")));
        assertTrue(nodes.contains(nodeFactory.textNode("Evelyn Waugh")));
        assertTrue(nodes.contains(nodeFactory.textNode("Herman Melville")));
        assertTrue(nodes.contains(nodeFactory.textNode("J. R. R. Tolkien")));
    }

    @Test
    public void testQuery_SimpleProperties_RecursiveDescent() {

        JsonQuery query = compiler.compile("$.store..price");
        List<JsonNode> nodes = query.execute(document);

        assertEquals(5, nodes.size());

        assertTrue(nodes.contains(nodeFactory.numberNode(8.95d)));
        assertTrue(nodes.contains(nodeFactory.numberNode(12.99d)));
        assertTrue(nodes.contains(nodeFactory.numberNode(8.99d)));
        assertTrue(nodes.contains(nodeFactory.numberNode(22.99d)));
        assertTrue(nodes.contains(nodeFactory.numberNode(19.95d)));
    }

    @Test
    public void testQuery_ArrayProperties() {

        JsonQuery query = compiler.compile("$.store.book[*].readers");
        List<JsonNode> nodes = query.execute(document);

        assertEquals(2, nodes.size());

        for (JsonNode node : nodes) {
            assertTrue(node instanceof ArrayNode);
        }

        List<JsonNode> nigelReesReaders = collectNodes((ArrayNode) nodes.get(0));
        assertTrue(nigelReesReaders.contains(createReader("Bob", 18)));
        assertTrue(nigelReesReaders.contains(createReader("Rob", 60)));

        List<JsonNode> evelynWaughReaders = collectNodes((ArrayNode) nodes.get(1));
        assertTrue(evelynWaughReaders.contains(createReader("John", 3)));
    }

    private JsonNode createReader(String name, Integer age) {

        ObjectNode reader = nodeFactory.objectNode();
        reader.set("name", nodeFactory.textNode(name));
        reader.set("age", nodeFactory.numberNode(age));
        return reader;
    }

    private List<JsonNode> collectNodes(ArrayNode arrayNode) {

        List<JsonNode> nodes = new ArrayList<>();

        Iterator<JsonNode> iter = arrayNode.elements();
        while (iter.hasNext()) {
            nodes.add(iter.next());
        }
        return nodes;
    }

    @Test
    public void testQuery_ArrayProperties_Flattened() {

        JsonQuery query = compiler.compile("$.store.book[*].readers.*");
        List<JsonNode> nodes = query.execute(document);

        assertEquals(3, nodes.size());

        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("Rob", 60)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_ArrayProperties_ByIndex_Brackets() {

        JsonQuery query = compiler.compile("$.store.book[*].readers[0]");
        List<JsonNode> nodes = query.execute(document);

        assertEquals(2, nodes.size());

        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_ArrayProperties_Dot() {

        JsonQuery query = compiler.compile("$.store.book[*].readers.1");
        List<JsonNode> nodes = query.execute(document);

        assertEquals(1, nodes.size());

        assertTrue(nodes.contains(createReader("Rob", 60)));
    }

    @Test
    public void testQuery_PredicateScript() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == $.allReaders[0].name)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Rob')]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(createReader("Rob", 60)));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues_InverseOrder() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?('Rob' != @.name)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues_Number() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.age == 60)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(createReader("Rob", 60)));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues_Boolean() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(true == true)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("Rob", 60)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_PredicateScript_LRAssociativity() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Rob' == true)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(createReader("Rob", 60)));
    }

    @Test
    public void testQuery_PredicateScript_LRAssociativity2() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Bob' && @.age == 18 || @.age == 3)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_PredicateScript_Precedence() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Bob' && @.age == 18 || @.name == 'John' && @.age == 3)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_PredicateScript_NestedScripts() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Bob' && (@.age == 18 || @.age == 3))]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
    }

    @Test
    public void testQuery_PredicateScript_NestedScripts2() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?((@.name) == 'Bob' && (@.age == 18 || @.age == 3) || @.name == $.allReaders[1].name && (60) == @.age)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("Rob", 60)));
    }

    @Test
    public void testQuery_FilterScript() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[(0)]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_FilterScript2() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].[($.properties[0])].*");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("Rob", 60)));
        assertTrue(nodes.contains(createReader("John", 3)));
    }

    @Test
    public void testQuery_Functions_Regex() {
        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[*][?(@.name =~ '.ob')]");

        List<JsonNode> nodes = query.execute(document);
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(createReader("Bob", 18)));
        assertTrue(nodes.contains(createReader("Rob", 60)));
    }
}
