package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.List;

class ConstantValue extends BaseQuery {

    private final JsonNode value;

    private ConstantValue(JsonNode value) {
        this.value = value;
    }

    @Override
    public List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {
        List<JsonNodeWrapper> result = new ArrayList<>(2);
        result.add(Utils.createWrapperNode(currentNode, value));
        return result;
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
