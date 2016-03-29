package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

class RecursiveDescent extends BaseQuery {

    private JsonQuery clientQuery;

    public RecursiveDescent(JsonQuery clientQuery) {
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        List<JsonNodeWrapper> nodes = new ArrayList<>();
        traverseNodes(rootNode, currentNode, nodes);
        return nodes;
    }

    public void traverseNodes(JsonNode rootNode, JsonNodeWrapper currentNode, List<JsonNodeWrapper> accumulator) {

        if (currentNode != null) {
            List<JsonNodeWrapper> nodes = clientQuery.execute(rootNode, currentNode);
            for (JsonNodeWrapper node : nodes) {
                if (node != null) {
                    accumulator.add(node);
                }
            }

            for (JsonNode childNode : currentNode.getNode()) {
                traverseNodes(rootNode, Utils.createWrapperNode(currentNode, childNode), accumulator);
            }
        }
    }
}
