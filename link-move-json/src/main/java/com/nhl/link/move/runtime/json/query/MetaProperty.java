package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.LmRuntimeException;

import java.util.Collections;
import java.util.List;

public class MetaProperty extends BaseQuery {

    public static final String NODE_PARENT = "parent";

    private JsonQuery clientQuery;
    private String propertyName;

    public MetaProperty(JsonQuery clientQuery, String propertyName) {
        this.clientQuery = clientQuery;
        this.propertyName = propertyName;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        JsonNodeWrapper node;
        switch (propertyName) {
            case NODE_PARENT: {
                node = currentNode.getParent();
                break;
            }
            default: {
                throw new LmRuntimeException("Unknown meta property: " + propertyName);
            }
        }

        if (node == null) {
            return Collections.emptyList();
        }

        return clientQuery == null?
                Collections.singletonList(node) : clientQuery.execute(rootNode, node);
    }
}
