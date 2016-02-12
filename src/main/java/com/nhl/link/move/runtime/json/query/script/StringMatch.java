package com.nhl.link.move.runtime.json.query.script;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.nhl.link.move.runtime.json.query.JsonQuery;

import java.util.regex.Pattern;

class StringMatch extends BinaryOp {

    StringMatch(JsonQuery lhsValueQuery, JsonQuery rhsValueQuery) {
        super(lhsValueQuery, rhsValueQuery);
    }

    @Override
    protected boolean apply(JsonNode lhsValue, JsonNode rhsValue) {

        if (lhsValue.getNodeType() != JsonNodeType.STRING) {
            return false;
        }

        // assume that regex is always on the right-hand side of this op
        Pattern pattern = Pattern.compile(rhsValue.asText());
        if (rhsValue.getNodeType() != JsonNodeType.STRING) {
            throw new RuntimeException("Not a regular expression: " + rhsValue.asText());
        }
        return pattern.matcher(lhsValue.asText()).matches();
    }
}
