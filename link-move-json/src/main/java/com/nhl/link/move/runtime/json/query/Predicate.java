package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class Predicate extends BaseQuery {

    private JsonQuery filter, clientQuery;

    Predicate(JsonQuery filter, JsonQuery clientQuery) {
        this.filter = filter;
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        List<JsonNodeWrapper> filteredNodes;
        JsonNode currentJsonNode = currentNode.getNode();
        if (currentJsonNode.isArray()) {
            filteredNodes = new ArrayList<>();
            for (JsonNode elementNode : currentJsonNode) {
                JsonNodeWrapper elementNodeWrapped = Utils.createWrapperNode(currentNode, elementNode);
                if (applyFilter(rootNode, elementNodeWrapped)) {
                    filteredNodes.add(elementNodeWrapped);
                }
            }
        } else {
            if (applyFilter(rootNode, currentNode)) {
                filteredNodes = Collections.singletonList(currentNode);
            } else {
                filteredNodes = Collections.emptyList();
            }
        }

        if (clientQuery == null) {
            return filteredNodes;
        } else {
            List<JsonNodeWrapper> result = new ArrayList<>(filteredNodes.size() + 1);
            for (JsonNodeWrapper filteredNode : filteredNodes) {
                result.addAll(clientQuery.execute(rootNode, filteredNode));
            }
            return result;
        }
    }

    private boolean applyFilter(JsonNode rootNode, JsonNodeWrapper elementNode) {

        List<JsonNodeWrapper> filterResult = filter.execute(rootNode, elementNode);
        if (Utils.isValueNode(filterResult)) {
            JsonNode valueNode = Utils.unwrapValueNode(filterResult);
            return !valueNode.isBoolean() || valueNode.asBoolean();
        } else {
            return !filterResult.isEmpty();
        }
    }
}
