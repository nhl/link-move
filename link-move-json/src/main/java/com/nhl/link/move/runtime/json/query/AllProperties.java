package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

class AllProperties extends BaseQuery {

    private JsonQuery clientQuery;

    public AllProperties(JsonQuery clientQuery) {
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        List<JsonNodeWrapper> nodes = new ArrayList<>();
        for (JsonNode childNode : currentNode.getNode()) {
            if (clientQuery != null) {
                nodes.addAll(clientQuery.execute(rootNode,
                        Utils.createWrapperNode(currentNode, childNode)));
            } else {
                nodes.add(Utils.createWrapperNode(currentNode, childNode));
            }
        }
        return nodes;
    }
}
