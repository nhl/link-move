package com.nhl.link.move.runtime.json.query.script;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.runtime.json.query.BaseQuery;
import com.nhl.link.move.runtime.json.query.JsonNodeWrapper;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.Utils;

import java.util.Collections;
import java.util.List;

abstract class BinaryOp extends BaseQuery {

    private static final List<JsonNodeWrapper> TRUE, FALSE;

    static {
        TRUE = Collections.singletonList(Utils.createWrapperNode(null, JsonNodeFactory.instance.booleanNode(true)));
        FALSE = Collections.singletonList(Utils.createWrapperNode(null, JsonNodeFactory.instance.booleanNode(false)));
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
    public final List<JsonNodeWrapper> doExecute(JsonNode rootNode, JsonNodeWrapper currentNode) {

        List<JsonNodeWrapper> leftResult = lhsValueQuery.execute(rootNode, currentNode);
        List<JsonNodeWrapper> rightResult = rhsValueQuery.execute(rootNode, currentNode);

        if (leftResult.isEmpty() || rightResult.isEmpty()) {
            return FALSE;
        }

        for (JsonNodeWrapper leftValue : leftResult) {
            if (Utils.isValueMissing(leftValue)) {
                return FALSE;
            }
            if (!leftValue.getNode().isValueNode()) {
                throw new LmRuntimeException(
                        "Expected (list of) value node(s) as a result of the left-hand side expression, but received: " +
                        leftValue.getNode().getNodeType().name());
            }
            for (JsonNodeWrapper rightValue : rightResult) {
                if (Utils.isValueMissing(rightValue)) {
                    return FALSE;
                }
                if (!rightValue.getNode().isValueNode()) {
                    throw new LmRuntimeException(
                            "Expected (list of) value node(s) as a result of the right-hand side expression, but received: " +
                            rightValue.getNode().getNodeType().name());
                }
                if (!apply(leftValue.getNode(), rightValue.getNode())) {
                    return FALSE;
                }
            }
        }
        return TRUE;
    }


}
