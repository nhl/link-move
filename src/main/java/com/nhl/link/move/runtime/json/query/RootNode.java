package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.List;

public class RootNode implements JsonQuery {

    private JsonQuery clientQuery;

    public RootNode(JsonQuery clientQuery) {
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode) {
        return execute(rootNode, rootNode);
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode, JsonNode currentNode) {

        if (currentNode == null) {
            return Collections.emptyList();
        }

        return clientQuery == null?
                Collections.singletonList(rootNode) : clientQuery.execute(rootNode, rootNode);
    }
}
