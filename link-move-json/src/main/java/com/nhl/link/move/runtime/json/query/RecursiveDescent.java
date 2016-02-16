package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

class RecursiveDescent implements JsonQuery {

    private JsonQuery clientQuery;

    public RecursiveDescent(JsonQuery clientQuery) {
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode) {
        return execute(rootNode, rootNode);
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode, JsonNode currentNode) {

        List<JsonNode> nodes = new ArrayList<>();
        traverseNodes(rootNode, currentNode, nodes);
        return nodes;
    }

    public void traverseNodes(JsonNode rootNode, JsonNode currentNode, List<JsonNode> accumulator) {

        if (currentNode != null) {
            List<JsonNode> nodes = clientQuery.execute(rootNode, currentNode);
            for (JsonNode node : nodes) {
                if (node != null) {
                    accumulator.add(node);
                }
            }

            for (JsonNode childNode : currentNode) {
                traverseNodes(rootNode, childNode, accumulator);
            }
        }
    }
}
