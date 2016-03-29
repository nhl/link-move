package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.List;

class NamedProperty extends BaseQuery {

    private JsonQuery clientQuery;
    private String propertyName;

    public NamedProperty(JsonQuery clientQuery, String propertyName) {
        this.clientQuery = clientQuery;
        this.propertyName = propertyName;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        JsonNode node = null;
        JsonNode currentJsonNode = currentNode.getNode();
        if (currentJsonNode.isArray()) {

            Integer index;
            try {
                index = Integer.valueOf(propertyName);
                node = currentJsonNode.get(index);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else {
            node = currentJsonNode.get(propertyName);
        }

        if (node == null) {
            return Collections.emptyList();
        }

        JsonNodeWrapper wrappedNode = Utils.createWrapperNode(currentNode, node);
        return clientQuery == null?
                Collections.singletonList(wrappedNode) : clientQuery.execute(rootNode, wrappedNode);
    }
}
