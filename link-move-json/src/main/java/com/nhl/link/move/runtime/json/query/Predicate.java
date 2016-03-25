package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

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
            List<JsonNode> result = new ArrayList<>(filteredNodes.size() + 1);
            for (JsonNode filteredNode : filteredNodes) {
                result.addAll(clientQuery.execute(rootNode, filteredNode));
            }
            return result;
        }
    }

    private boolean applyFilter(JsonNode rootNode, JsonNode elementNode) {

        List<JsonNode> filterResult = filter.execute(rootNode, elementNode);
        if (Utils.isValueNode(filterResult)) {
            JsonNode valueNode = Utils.unwrapValueNode(filterResult);
            return !valueNode.isBoolean() || valueNode.asBoolean();
        } else {
            return !filterResult.isEmpty();
        }
    }
}
