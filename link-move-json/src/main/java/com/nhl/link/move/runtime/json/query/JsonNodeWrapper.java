package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;

public interface JsonNodeWrapper {

    JsonNodeWrapper getParent();
    JsonNode getNode();
}
