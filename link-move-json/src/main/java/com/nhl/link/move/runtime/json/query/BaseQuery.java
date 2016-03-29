package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.List;

public abstract class BaseQuery implements JsonQuery {

    @Override
    public List<JsonNodeWrapper> execute(JsonNode rootNode) {
        return execute(rootNode, Utils.createWrapperNode(null, rootNode));
    }

    @Override
    public List<JsonNodeWrapper> execute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        if (currentNode.getNode() == null) {
            return Collections.emptyList();
        }
        return doExecute(rootNode, currentNode);
    }

    protected abstract List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode);
}
