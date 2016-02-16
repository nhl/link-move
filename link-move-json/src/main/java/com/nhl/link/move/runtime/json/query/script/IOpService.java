package com.nhl.link.move.runtime.json.query.script;

import com.nhl.link.move.runtime.json.query.JsonQuery;

public interface IOpService {

    JsonQuery buildOp(String name, JsonQuery lhsValueQuery, JsonQuery rhsValueQuery);
    boolean isOp(String literal);
}
