package com.nhl.link.move.runtime.json.query.script;

import com.nhl.link.move.runtime.json.query.JsonQuery;

import java.util.HashMap;
import java.util.Map;

public class OpService implements IOpService {

    private Map<String, Class<? extends BinaryOp>> operators;

    public OpService() {

        operators = new HashMap<>();
        operators.put("==", Eq.class);
        operators.put("!=", Neq.class);
        operators.put("&&", And.class);
        operators.put("||", Or.class);
        operators.put(">", Gt.class);
        operators.put(">=", Gte.class);
        operators.put("<=", Lte.class);
        operators.put("<", Lt.class);
        operators.put("=~", StringMatch.class);
    }

    public JsonQuery buildOp(String name, JsonQuery lhsValueQuery, JsonQuery rhsValueQuery) {

        if (!isOp(name)) {
            throw new RuntimeException("Unknown operator: " + name);
        }

        Class<? extends BinaryOp> opClass = operators.get(name);
        try {
            return opClass.getDeclaredConstructor(JsonQuery.class, JsonQuery.class).newInstance(lhsValueQuery, rhsValueQuery);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build op: " + name, e);
        }
    }

    public boolean isOp(String literal) {
        return operators.containsKey(literal);
    }
}
