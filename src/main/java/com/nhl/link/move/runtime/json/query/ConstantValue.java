package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.List;

class ConstantValue implements JsonQuery {

    private final List<JsonNode> wrappedValue;

    private ConstantValue(JsonNode value) {
        wrappedValue = new ArrayList<>(2);
        wrappedValue.add(value);
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode) {
        return wrappedValue;
    }

    @Override
    public List<JsonNode> execute(JsonNode rootNode, JsonNode currentNode) {
        return wrappedValue;
    }

    public static ConstantValue valueOf(String value) {
        return new ConstantValue(JsonNodeFactory.instance.textNode(value));
    }

    public static ConstantValue valueOf(Integer value) {
        return new ConstantValue(JsonNodeFactory.instance.numberNode(value));
    }

    public static ConstantValue valueOf(Boolean value) {
        return new ConstantValue(JsonNodeFactory.instance.booleanNode(value));
    }
}
