package com.nhl.link.move.runtime.json.query.script;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.Utils;

import java.util.Collections;
import java.util.List;

abstract class BinaryOp implements JsonQuery {

    private static final List<JsonNode> TRUE, FALSE;

    static {
        TRUE = Collections.<JsonNode>singletonList(JsonNodeFactory.instance.booleanNode(true));
        FALSE = Collections.<JsonNode>singletonList(JsonNodeFactory.instance.booleanNode(false));
    }

    private JsonQuery lhsValueQuery, rhsValueQuery;

    BinaryOp(JsonQuery lhsValueQuery, JsonQuery rhsValueQuery) {

        if (lhsValueQuery == null || rhsValueQuery == null) {
            throw new RuntimeException("Both left-hand and right-hand sides must be present");
        }

        this.lhsValueQuery = lhsValueQuery;
        this.rhsValueQuery = rhsValueQuery;
    }

    protected abstract boolean apply(JsonNode lhsValue, JsonNode rhsValue);

    @Override
    public final List<JsonNode> execute(JsonNode rootNode) {
        return execute(rootNode, rootNode);
    }

    @Override
    public final List<JsonNode> execute(JsonNode rootNode, JsonNode currentNode) {

        JsonNode leftValue = Utils.unwrapValueNode(lhsValueQuery.execute(rootNode, currentNode));
        JsonNode rightValue = Utils.unwrapValueNode(rhsValueQuery.execute(rootNode, currentNode));

        if (isValueMissing(leftValue) || isValueMissing(rightValue)) {
            return FALSE;
        }
        return apply(leftValue, rightValue) ? TRUE : FALSE;
    }

    private static boolean isValueMissing(JsonNode node) {
        return node == null || node.getNodeType() == JsonNodeType.MISSING || node.getNodeType() == JsonNodeType.NULL;
    }
}
