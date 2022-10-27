package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.runtime.json.query.JsonNodeWrapper;
import com.nhl.link.move.runtime.json.query.JsonQuery;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;

class JsonExtractor implements Extractor {

    private final StreamConnector connector;
    private final JsonRowAttribute[] rowHeader;
    private final String queryString;
    private final JsonQuery query;

    private final IJacksonService jacksonService;

    public JsonExtractor(
            IJacksonService jacksonService,
            StreamConnector connector,
            JsonRowAttribute[] rowHeader,
            String queryString,
            JsonQuery query) {

        this.jacksonService = jacksonService;
        this.connector = connector;

        // TODO: use JSON properties for the header if not set
        this.rowHeader = Objects.requireNonNull(rowHeader, "An explicit 'rowHeader' is currently required to process a JSON source");
        this.queryString = queryString;
        this.query = query;
    }

    @Override
    public RowReader getReader(Execution exec) {
        try {
            JsonNode source;
            try (InputStream in = connector.getInputStream(exec.getParameters())) {
                source = jacksonService.parseJson(in);
            }
            List<JsonNodeWrapper> nodes = query.execute(source);

            exec.getLogger().extractorStarted(rowHeader, queryString);
            return new JsonRowReader(rowHeader, source, nodes);
        } catch (Exception e) {
            throw new LmRuntimeException(e);
        }
    }
}
