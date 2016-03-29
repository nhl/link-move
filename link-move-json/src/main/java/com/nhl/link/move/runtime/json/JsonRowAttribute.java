package com.nhl.link.move.runtime.json;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.QueryCompiler;

public class JsonRowAttribute implements RowAttribute {

    private RowAttribute attribute;
    private JsonQuery sourceQuery;

    public JsonRowAttribute(RowAttribute attribute, QueryCompiler compiler) {
        this.attribute = attribute;

        String sourceName = attribute.getSourceName();
        try {
            this.sourceQuery = compiler.compile(sourceName);
        } catch (Exception e) {
            throw new LmRuntimeException("Invalid JsonPath query: " + sourceName, e);
        }
    }

    @Override
    public int getOrdinal() {
        return attribute.getOrdinal();
    }

    @Override
    public Class<?> type() {
        return attribute.type();
    }

    @Override
    public String getSourceName() {
        return attribute.getSourceName();
    }

    @Override
    public String getTargetPath() {
        return attribute.getTargetPath();
    }

    public JsonQuery getSourceQuery() {
        return sourceQuery;
    }
}
