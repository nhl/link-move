package com.nhl.link.move.runtime.json.query.script;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.nhl.link.move.LmRuntimeException;
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

        List<JsonNode> leftResult = lhsValueQuery.execute(rootNode, currentNode);
        List<JsonNode> rightResult = rhsValueQuery.execute(rootNode, currentNode);

        if (leftResult.isEmpty() || rightResult.isEmpty()) {
            return FALSE;
        }

        for (JsonNode leftValue : leftResult) {
            if (Utils.isValueMissing(leftValue)) {
                return FALSE;
            }
            if (!leftValue.isValueNode()) {
                throw new LmRuntimeException(
                        "Expected (list of) value node(s) as a result of the left-hand side expression, but received: " +
                        leftValue.getNodeType().name());
            }
            for (JsonNode rightValue : rightResult) {
                if (Utils.isValueMissing(rightValue)) {
                    return FALSE;
                }
                if (!rightValue.isValueNode()) {
                    throw new LmRuntimeException(
                            "Expected (list of) value node(s) as a result of the right-hand side expression, but received: " +
                            rightValue.getNodeType().name());
                }
                if (!apply(leftValue, rightValue)) {
                    return FALSE;
                }
            }
        }
        return TRUE;
    }


}
