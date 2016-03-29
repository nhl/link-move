package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.runtime.json.query.JsonNodeWrapper;
import com.nhl.link.move.runtime.json.query.JsonQuery;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

class JsonExtractor implements Extractor {

    private final StreamConnector connector;
	private final JsonRowAttribute[] attributes;
	private final JsonQuery query;

    private IJacksonService jacksonService;

    public JsonExtractor(IJacksonService jacksonService, StreamConnector connector,
                         JsonRowAttribute[] attributes, JsonQuery query) {

        this.jacksonService = jacksonService;
		this.connector = connector;
		this.attributes = attributes;
		this.query = query;
	}

    @Override
    public RowReader getReader(Map<String, ?> parameters) {
        try {
            JsonNode source;
            try (InputStream in = connector.getInputStream()) {
                source = jacksonService.parseJson(in);
            }
            List<JsonNodeWrapper> nodes = query.execute(source);
			return new JsonRowReader(attributes, source, nodes);
		} catch (Exception e) {
			throw new LmRuntimeException(e);
		}
    }
}
