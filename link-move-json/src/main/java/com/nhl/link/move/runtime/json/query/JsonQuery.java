package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public interface JsonQuery {

    // TODO: boolean isConstant(); for optimization
    List<JsonNodeWrapper> execute(JsonNode rootNode);
    List<JsonNodeWrapper> execute(JsonNode rootNode, JsonNodeWrapper currentNode);
}
