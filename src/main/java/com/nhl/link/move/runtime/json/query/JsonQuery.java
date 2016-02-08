package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public interface JsonQuery {

    List<JsonNode> execute(JsonNode rootNode);
    List<JsonNode> execute(JsonNode rootNode, JsonNode currentNode);
}
