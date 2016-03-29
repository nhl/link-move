package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.List;

class CurrentNode extends BaseQuery {

    private JsonQuery clientQuery;

    public CurrentNode(JsonQuery clientQuery) {
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        return clientQuery == null?
                Collections.singletonList(currentNode) : clientQuery.execute(rootNode, currentNode);
    }
}
