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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JsonQueryTest {

    private static JsonNodeFactory nodeFactory;
    private QueryCompiler compiler;
    private JsonNode document;
    
    private enum Readers {
        Bob("Bob", 18, "God save the Queen!"),
        Rob("Rob", 60, null),
        John("John", 3, "Goo goo ga ga");
        
        private JsonNode node;
        
        Readers(String name, Integer age, String motto) {
            
            ObjectNode reader = nodeFactory.objectNode();
            reader.set("name", nodeFactory.textNode(name));
            reader.set("age", nodeFactory.numberNode(age));
            if (motto != null) {
                reader.set("motto", nodeFactory.textNode(motto));
            }
            this.node = reader;
        }
        
        public JsonNode toJson() {
            return node;
        }
    }

    @Before
    public void setUp() throws IOException {

        nodeFactory = JsonNodeFactory.instance;
        compiler = new QueryCompiler();
        
        StringBuilder jsonSb = new StringBuilder();
        {
            Scanner scanner = new Scanner(JsonQueryTest.class.getResourceAsStream("document.json"));
            while (scanner.hasNextLine()) {
            	jsonSb.append(scanner.nextLine());
            }
            scanner.close();        	
        }
        
        document = new JacksonService().parseJson(jsonSb.toString());
    }

    @Test
    public void testQuery_SimpleProperties_Dot() {

        JsonQuery query = compiler.compile("$.store.book[*].author");
        List<JsonNode> nodes = collectJsonNodes(query.execute(document));

        assertEquals(4, nodes.size());

        assertTrue(nodes.contains(nodeFactory.textNode("Nigel Rees")));
        assertTrue(nodes.contains(nodeFactory.textNode("Evelyn Waugh")));
        assertTrue(nodes.contains(nodeFactory.textNode("Herman Melville")));
        assertTrue(nodes.contains(nodeFactory.textNode("J. R. R. Tolkien")));
    }

    @Test
    public void testQuery_SimpleProperties_Brackets() {

        JsonQuery query = compiler.compile("$['store'][\"book\"][*]['author']");
        List<JsonNode> nodes = collectJsonNodes(query.execute(document));

        assertEquals(4, nodes.size());

        assertTrue(nodes.contains(nodeFactory.textNode("Nigel Rees")));
        assertTrue(nodes.contains(nodeFactory.textNode("Evelyn Waugh")));
        assertTrue(nodes.contains(nodeFactory.textNode("Herman Melville")));
        assertTrue(nodes.contains(nodeFactory.textNode("J. R. R. Tolkien")));
    }

    @Test
    public void testQuery_SimpleProperties_RecursiveDescent() {

        JsonQuery query = compiler.compile("$.store..price");
        List<JsonNode> nodes = collectJsonNodes(query.execute(document));

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
        List<JsonNode> nodes = collectJsonNodes(query.execute(document));

        assertEquals(2, nodes.size());

        for (JsonNode node : nodes) {
            assertTrue(node instanceof ArrayNode);
        }

        List<JsonNode> nigelReesReaders = collectNodes((ArrayNode) nodes.get(0));
        assertTrue(nigelReesReaders.contains(Readers.Bob.toJson()));
        assertTrue(nigelReesReaders.contains(Readers.Rob.toJson()));

        List<JsonNode> evelynWaughReaders = collectNodes((ArrayNode) nodes.get(1));
        assertTrue(evelynWaughReaders.contains(Readers.John.toJson()));
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
        List<JsonNode> nodes = collectJsonNodes(query.execute(document));

        assertEquals(3, nodes.size());

        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.Rob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }
    
    @Test
    public void testQueries_CompilerBehaviourImmutable() {

      JsonQuery query;
      List<JsonNode> nodes;

      {
            query = compiler.compile("$.store.book[*].readers[*]"); // readers[*] means all items of the book array.
            nodes = collectJsonNodes(query.execute(document));

            assertEquals(3, nodes.size());

            assertTrue(nodes.contains(Readers.Bob.toJson()));
            assertTrue(nodes.contains(Readers.Rob.toJson()));
            assertTrue(nodes.contains(Readers.John.toJson()));
      }

      {
            query = compiler.compile("$.store.book[*]");
            nodes = collectJsonNodes(query.execute(document));

            assertEquals(4, nodes.size());
      }

      {
            query = compiler.compile("$.store.book[*].readers[*]");
            nodes = collectJsonNodes(query.execute(document));

            assertEquals(3, nodes.size());

            assertTrue(nodes.contains(Readers.Bob.toJson()));
            assertTrue(nodes.contains(Readers.Rob.toJson()));
            assertTrue(nodes.contains(Readers.John.toJson()));
      }
      
      {
          query = compiler.compile("$.store.book[*]");
          nodes = collectJsonNodes(query.execute(document));

          assertEquals(4, nodes.size());
      }
    }

    @Test
    public void testQuery_ArrayProperties_ByIndex_Brackets() {

        JsonQuery query = compiler.compile("$.store.book[*].readers[0]");
        List<JsonNode> nodes = collectJsonNodes(query.execute(document));

        assertEquals(2, nodes.size());

        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_ArrayProperties_Dot() {

        JsonQuery query = compiler.compile("$.store.book[*].readers.1");
        List<JsonNode> nodes = collectJsonNodes(query.execute(document));

        assertEquals(1, nodes.size());

        assertTrue(nodes.contains(Readers.Rob.toJson()));
    }

    @Test
    public void testQuery_PredicateScript() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == $.allReaders[0].name)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Rob')]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(Readers.Rob.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues_InverseOrder() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?('Rob' != @.name)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues_Number() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.age == 60)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(Readers.Rob.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_ConstantValues_Boolean() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(true == true)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.Rob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_LRAssociativity() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Rob' == true)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(Readers.Rob.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_LRAssociativity2() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Bob' && @.age == 18 || @.age == 3)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_Precedence() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Bob' && @.age == 18 || @.name == 'John' && @.age == 3)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_NestedScripts() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?(@.name == 'Bob' && (@.age == 18 || @.age == 3))]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
    }

    @Test
    public void testQuery_PredicateScript_NestedScripts2() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[?((@.name) == 'Bob' && (@.age == 18 || @.age == 3) || @.name == $.allReaders[1].name && (60) == @.age)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.Rob.toJson()));
    }

    @Test
    public void testQuery_FilterScript() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[(0)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_FilterScript2() {

        JsonQuery query = compiler.compile(
                "$.store.book[*].[($.properties[0])].*");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.Rob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_Functions_Regex() {
        JsonQuery query = compiler.compile(
                "$.store.book[*].readers[*][?(@.name =~ '.ob')]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.Rob.toJson()));
    }

    @Test
    public void testQuery_UntypedPredicate1() {

        JsonQuery query = compiler.compile(
                "$.store.book[?(@.readers[?(@.age != 3)])]");

        // TODO: BinaryOp should support collections as part of the expression
        // e.g.: $.store.book[?(@.readers[*].age != 3)]
        // this will require defining semantics for all
        // possible combinations of scalars and collections

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertEquals("Sayings of the Century", nodes.get(0).get("title").asText());
    }

    @Test
    public void testQuery_UntypedPredicate2() {

        JsonQuery query = compiler.compile(
                "$.store..readers[?(@.motto)]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_UntypedPredicate3() {

        JsonQuery query = compiler.compile(
                "$.store.book[?(@.readers[*].age <= 18)].readers[*]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_UntypedPredicate4() {

        JsonQuery query = compiler.compile(
                "$.store.book[?(@.readers[*].age == 3 || @.readers[*].age >= 18)].readers[*]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(Readers.Bob.toJson()));
        assertTrue(nodes.contains(Readers.Rob.toJson()));
        assertTrue(nodes.contains(Readers.John.toJson()));
    }

    @Test
    public void testQuery_Script_MissingProperties1() {

        JsonQuery query = compiler.compile("$.store.book[?(@.someProperty == 'someValue')]");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(0, nodes.size());
    }

    @Test
    public void testQuery_Script_Parents1() {

        JsonQuery query = compiler.compile("$.store.book..readers#parent");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(2, nodes.size());

        List<String> titles = new ArrayList<>(3);
        for (JsonNode node : nodes) {
            titles.add(node.get("title").textValue());
        }
        assertTrue(titles.containsAll(Arrays.asList("Sayings of the Century", "Sword of Honour")));
    }

    @Test
    public void testQuery_Script_Parents2() {

        JsonQuery query = compiler.compile("$.store.book#parent#parent");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(1, nodes.size());
        assertNotNull(nodes.get(0).get("store"));
    }

    @Test
    public void testQuery_Script_Parents3() {

        JsonQuery query = compiler.compile("$#parent");

        List<JsonNode> nodes = collectJsonNodes(query.execute(document));
        assertEquals(0, nodes.size());
    }

    @Test
    public void testQuery_Script_Parents4() {

        JsonQuery query = compiler.compile("$..*#parent");

        Set<JsonNode> nodes = new HashSet<>(collectJsonNodes(query.execute(document)));
        Set<JsonNode> parents = collectParents(document);
        assertEquals(parents.size(), nodes.size());
        assertTrue(nodes.containsAll(parents));
    }

    private Set<JsonNode> collectParents(JsonNode node) {

        Set<JsonNode> acc = new HashSet<>();
        if (node.size() > 0) {
            acc.add(node);
        }

        for (JsonNode child : node) {
            acc.addAll(collectParents(child));
        }
        return acc;
    }

    private List<JsonNode> collectJsonNodes(List<JsonNodeWrapper> wrappers) {

        List<JsonNode> nodes = new ArrayList<>(wrappers.size() + 1);
        for (JsonNodeWrapper wrapper : wrappers) {
            nodes.add(wrapper.getNode());
        }
        return nodes;
    }

}
