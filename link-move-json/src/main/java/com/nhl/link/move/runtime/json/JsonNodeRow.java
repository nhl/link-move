package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;

import java.util.List;

class JsonNodeRow implements Row {

    private final JsonRowAttribute[] attributes;
	private final JsonNode rootNode, currentNode;

	public JsonNodeRow(JsonRowAttribute[] attributes, JsonNode rootNode, JsonNode currentNode) {
		this.attributes = attributes;
		this.rootNode = rootNode;
		this.currentNode = currentNode;
	}

	@Override
	public Object get(RowAttribute attribute) {

		// TODO: remove cast (maybe Row should have a generic type argument,
		// indicating the type of it's attributes? this will require global refactoring though)
		JsonRowAttribute jsonAttribute = (JsonRowAttribute) attribute;

		List<JsonNode> result = jsonAttribute.getSourceQuery().execute(rootNode, currentNode);
		if (result == null || result.isEmpty()) {
			return null;
		}
		if (result.size() > 1) {
			throw new LmRuntimeException("Attribute query yielded a list of values (total: " + result.size() +
					"). A single value is expected.");
		}

		JsonNode value = result.get(0);
		if (value.isObject() || value.isArray()) {
			throw new LmRuntimeException("Expected a value node");
		}
		return value.asText();
	}

	@Override
	public RowAttribute[] attributes() {
		return attributes;
	}
}
