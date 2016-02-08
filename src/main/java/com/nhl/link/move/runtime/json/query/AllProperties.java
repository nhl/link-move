package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AllProperties implements JsonQuery {

    private JsonQuery clientQuery;

    public AllProperties(JsonQuery clientQuery) {
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

        List<JsonNode> nodes = new ArrayList<>();
        for (JsonNode childNode : currentNode) {
            if (clientQuery != null) {
                nodes.addAll(clientQuery.execute(rootNode, childNode));
            } else {
                nodes.add(childNode);
            }
        }
        return nodes;
    }
}
