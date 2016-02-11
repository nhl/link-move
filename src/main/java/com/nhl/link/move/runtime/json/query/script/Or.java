package com.nhl.link.move.runtime.json.query.script;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.runtime.json.query.JsonQuery;

class Or extends BinaryOp {

    Or(JsonQuery lhsValueQuery, JsonQuery rhsValueQuery) {
        super(lhsValueQuery, rhsValueQuery);
    }

    @Override
    protected boolean apply(JsonNode lhsValue, JsonNode rhsValue) {

        if (lhsValue.isBoolean() && rhsValue.isBoolean()) {
            return lhsValue.asBoolean() || rhsValue.asBoolean();
        } else {
            throw new RuntimeException("Both arguments must be boolean");
        }
    }
}
