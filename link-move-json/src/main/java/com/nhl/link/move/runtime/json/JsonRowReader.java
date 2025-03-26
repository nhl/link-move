package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.runtime.json.query.JsonNodeWrapper;

import java.util.Iterator;
import java.util.List;

class JsonRowReader implements RowReader {

    private final JsonRowAttribute[] header;
    private final JsonNode rootNode;
    private final List<JsonNodeWrapper> items;

    public JsonRowReader(JsonRowAttribute[] header, JsonNode rootNode, List<JsonNodeWrapper> items) {
        this.header = header;
        this.rootNode = rootNode;
        this.items = items;
    }

    @Override
    public void close() {
        // no need to close anything
    }

    @Override
    public JsonRowAttribute[] getHeader() {
        return header;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new Iterator<>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < items.size();
            }

            @Override
            public Object[] next() {
                return fromNode(items.get(i++));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Object[] fromNode(JsonNodeWrapper node) {

        Object[] row = new Object[header.length];

        for (int i = 0; i < header.length; i++) {
            row[i] = valueFromNode(header[i], node);
        }

        return row;
    }

    private Object valueFromNode(JsonRowAttribute attribute, JsonNodeWrapper node) {

        List<JsonNodeWrapper> result = attribute.getSourceQuery().execute(rootNode, node);
        if (result == null || result.isEmpty()) {
            return null;
        }
        if (result.size() > 1) {
            throw new LmRuntimeException("Attribute query yielded a list of values (total: " + result.size() +
                    "). A single value is expected.");
        }
        return extractValue(result.get(0).getNode());
    }

    private Object extractValue(JsonNode node) {

        // extract values where we can; for the rest just return the unchanged JSON node

        switch (node.getNodeType()) {
            case STRING:
                return node.asText();
            case BOOLEAN:
                return node.asBoolean();
            case NUMBER: {
                NumericNode numericNode = (NumericNode) node;
                switch (numericNode.numberType()) {
                    case INT: {
                        return numericNode.intValue();
                    }
                    case LONG: {
                        return numericNode.longValue();
                    }
                    case FLOAT: {
                        return numericNode.floatValue();
                    }
                    case DOUBLE: {
                        return numericNode.doubleValue();
                    }
                    case BIG_INTEGER: {
                        return numericNode.bigIntegerValue();
                    }
                    case BIG_DECIMAL: {
                        return numericNode.decimalValue();
                    }
                    // intentionally fall through
                }
            }
            case NULL:
            case MISSING:
                return null;
            default:
                return node;
        }
    }

}
