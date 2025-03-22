package com.nhl.link.move.mapper;

import org.dflib.row.RowProxy;
import org.apache.cayenne.exp.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.cayenne.exp.ExpressionFactory.joinExp;

public class MultiPathMapper implements Mapper {

    private final Map<String, Mapper> pathMappers;

    public MultiPathMapper(Map<String, Mapper> pathMappers) {
        this.pathMappers = pathMappers;
    }

    @Override
    public Expression expressionForKey(Object key) {

        Map<String, Object> keyMap = (Map<String, Object>) key;

        List<Expression> clauses = new ArrayList<>(pathMappers.size());

        for (Entry<String, Mapper> e : pathMappers.entrySet()) {
            Object value = keyMap.get(e.getKey());
            clauses.add(e.getValue().expressionForKey(value));
        }

        return joinExp(Expression.AND, clauses);
    }

    @Override
    public Object keyForSource(RowProxy source) {
        Map<String, Object> keyMap = new HashMap<>(pathMappers.size() * 2);
        for (Entry<String, Mapper> e : pathMappers.entrySet()) {
            keyMap.put(e.getKey(), e.getValue().keyForSource(source));
        }

        return keyMap;
    }

    @Override
    public Object keyForTarget(Object target) {
        Map<String, Object> keyMap = new HashMap<>(pathMappers.size() * 2);

        for (Entry<String, Mapper> e : pathMappers.entrySet()) {
            keyMap.put(e.getKey(), e.getValue().keyForTarget(target));
        }
        return keyMap;
    }
}
