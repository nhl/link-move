package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.List;

class RootNode extends BaseQuery {

    private JsonQuery clientQuery;

    public RootNode(JsonQuery clientQuery) {
        this.clientQuery = clientQuery;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        return clientQuery == null?
                Collections.singletonList(Utils.createWrapperNode(null, rootNode)) : clientQuery.execute(rootNode);
    }
}
