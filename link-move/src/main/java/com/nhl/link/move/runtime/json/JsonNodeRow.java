package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;

class JsonNodeRow implements Row {

    private final RowAttribute[] attributes;
	private final JsonNode node;

	public JsonNodeRow(RowAttribute[] attributes, JsonNode node) {
		this.attributes = attributes;
		this.node = node;
	}

	@Override
	public Object get(RowAttribute attribute) {

		JsonNode value = node.get(attribute.getSourceName());
		if (value == null) {
			return null;
		}
		// TODO: eventually we might want to support individual expressions
		// to access nested objects/properties
		if (value.isObject() || value.isArray()) {
			throw new LmRuntimeException("Expected value or binary node");
		}
		return value.asText();
	}

	@Override
	public RowAttribute[] attributes() {
		return attributes;
	}
}
