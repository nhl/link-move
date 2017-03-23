package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.runtime.json.query.JsonNodeWrapper;

import java.util.List;

class JsonNodeRow implements Row {

    private final JsonRowAttribute[] attributes;
	private final JsonNode rootNode;
	private final JsonNodeWrapper currentNode;

	public JsonNodeRow(JsonRowAttribute[] attributes, JsonNode rootNode, JsonNodeWrapper currentNode) {
		this.attributes = attributes;
		this.rootNode = rootNode;
		this.currentNode = currentNode;
	}

	@Override
	public Object get(RowAttribute attribute) {

		// TODO: remove cast (maybe Row should have a generic type argument,
		// indicating the type of it's attributes? this will require global refactoring though)
		JsonRowAttribute jsonAttribute = (JsonRowAttribute) attribute;

		List<JsonNodeWrapper> result = jsonAttribute.getSourceQuery().execute(rootNode, currentNode);
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
		switch (node.getNodeType()) {
			case ARRAY:
			case POJO:
			case OBJECT: {
				throw new LmRuntimeException("Expected a value node");
			}
			case MISSING:
			case NULL: {
				return null;
			}
			case BINARY:
			case STRING: {
				return node.asText();
			}
			case BOOLEAN: {
				return node.asBoolean();
			}
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
			default: {
				throw new LmRuntimeException("Unexpected JSON node type: " + node.getNodeType());
			}
		}
	}

	@Override
	public RowAttribute[] attributes() {
		return attributes;
	}
}
