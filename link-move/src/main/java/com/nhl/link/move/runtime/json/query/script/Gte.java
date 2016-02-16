package com.nhl.link.move.runtime.json.query.script;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.Utils;

class Gte extends BinaryOp {

    Gte(JsonQuery lhsValueQuery, JsonQuery rhsValueQuery) {
        super(lhsValueQuery, rhsValueQuery);
    }

    @Override
    protected boolean apply(JsonNode lhsValue, JsonNode rhsValue) {
        return Utils.compare(lhsValue, rhsValue) >= 0;
    }
}
