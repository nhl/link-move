package com.nhl.link.move.mapper;

import com.nhl.dflib.row.RowProxy;
import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;

public class PathMapper implements Mapper {

    private final String dbPath;

    // created lazily and cached... parsing expressions is expensive
    private Expression pathExpression;
    private Expression keyValueExpression;

    public PathMapper(String dbPath) {
        this.dbPath = dbPath;
    }

    private Expression getOrCreatePathExpression() {
        if (pathExpression == null) {
            // as we expect both Db and Obj paths here, let's pass the path
            // through the parser to generate the correct expression template...
            this.pathExpression = ExpressionFactory.exp(dbPath);
        }

        return pathExpression;
    }

    private Expression getOrCreateKeyValueExpression() {
        if (keyValueExpression == null) {
            // as we expect both Db and Obj paths here, let's pass the path
            // through the parser to generate the correct expression template...
            this.keyValueExpression = ExpressionFactory.exp(dbPath + " = $v");
        }

        return keyValueExpression;
    }

    @Override
    public Expression expressionForKey(Object key) {
        return getOrCreateKeyValueExpression().paramsArray(key);
    }

    @Override
    public Object keyForSource(RowProxy source) {

        // if source does not contain a key, we must fail, otherwise multiple
        // rows will be incorrectly matched against NULL key

        try {
            return source.get(dbPath);
        }
        catch (IllegalArgumentException e) {
            throw new LmRuntimeException("Source does not contain key path: " + dbPath);
        }
    }

    @Override
    public Object keyForTarget(Object target) {

        // cases:
        // 1. "obj:" expressions are object properties
        // 2. "db:" expressions mapping to ID columns
        // 3. "db:" expressions mapping to object properties

        // Cayenne exp can handle 1 & 2; we'll need to manually handle case 3

        return getOrCreatePathExpression().evaluate(target);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "__" + dbPath;
    }
}
