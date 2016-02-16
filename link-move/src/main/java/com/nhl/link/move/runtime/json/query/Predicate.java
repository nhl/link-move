package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class Predicate implements JsonQuery {

    private JsonQuery filter, clientQuery;

    Predicate(JsonQuery filter, JsonQuery clientQuery) {
        this.filter = filter;
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode) {
        return execute(rootNode, rootNode);
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode, JsonNode currentNode) {

        List<JsonNode> filteredNodes;

        if (currentNode.isArray()) {
            filteredNodes = new ArrayList<>();
            for (JsonNode elementNode : currentNode) {
                if (applyFilter(rootNode, elementNode)) {
                    filteredNodes.add(elementNode);
                }
            }
        } else if (applyFilter(rootNode, currentNode)) {
            filteredNodes = Collections.singletonList(currentNode);
        } else {
            filteredNodes = Collections.emptyList();
        }

        if (clientQuery == null) {
            return filteredNodes;
        } else {
            return clientQuery.execute(rootNode, toArrayNode(filteredNodes));
        }
    }

    private boolean applyFilter(JsonNode rootNode, JsonNode elementNode) {

        JsonNode filterResult = Utils.unwrapValueNode(filter.execute(rootNode, elementNode));
        if (!filterResult.isBoolean()) {
            throw new RuntimeException("Unexpected value received from filter, expected boolean");
        }
        return filterResult.asBoolean();
    }

    private static ArrayNode toArrayNode(List<JsonNode> nodes) {

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        for (JsonNode node : nodes) {
            arrayNode.add(node);
        }
        return arrayNode;
    }
}
