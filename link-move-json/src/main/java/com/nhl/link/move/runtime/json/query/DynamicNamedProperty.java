package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.List;

class DynamicNamedProperty extends BaseQuery {

    private JsonQuery valueQuery, clientQuery;

    DynamicNamedProperty(JsonQuery valueQuery, JsonQuery clientQuery) {
        this.valueQuery = valueQuery;
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        JsonNode propertyNameNode = Utils.unwrapValueNode(valueQuery.execute(rootNode, currentNode));
        if (propertyNameNode == null) {
            return Collections.emptyList();
        }

        JsonQuery delegate = new NamedProperty(clientQuery, propertyNameNode.asText());
        return delegate.execute(rootNode, currentNode);
    }
}
