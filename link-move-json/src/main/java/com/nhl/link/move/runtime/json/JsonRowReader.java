package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.runtime.json.query.JsonNodeWrapper;

import java.util.Iterator;
import java.util.List;

class JsonRowReader implements RowReader {

    private final JsonRowAttribute[] attributes;
	private final JsonNode rootNode;
	private final List<JsonNodeWrapper> items;

	public JsonRowReader(JsonRowAttribute[] attributes, JsonNode rootNode, List<JsonNodeWrapper> items) {
		this.attributes = attributes;
		this.rootNode = rootNode;
		this.items = items;
	}

	@Override
	public void close() {
		// no need to close anything
	}

	@Override
	public Iterator<Row> iterator() {
		return new Iterator<Row>() {
			private int i = 0;

			@Override
			public boolean hasNext() {
				return i < items.size();
			}

			@Override
			public Row next() {
				JsonNodeWrapper currentNode = items.get(i++);
				return new JsonNodeRow(attributes, rootNode, currentNode);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
