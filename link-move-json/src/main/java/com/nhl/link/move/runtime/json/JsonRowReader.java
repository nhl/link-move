package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;

import java.util.Iterator;
import java.util.List;

class JsonRowReader implements RowReader {

    private final RowAttribute[] attributes;
	private final List<JsonNode> nodes;

	public JsonRowReader(RowAttribute[] attributes, List<JsonNode> nodes) {
		this.attributes = attributes;
		this.nodes = nodes;
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
				return i < nodes.size();
			}

			@Override
			public Row next() {
				JsonNode node = nodes.get(i++);
				return new JsonNodeRow(attributes, node);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
