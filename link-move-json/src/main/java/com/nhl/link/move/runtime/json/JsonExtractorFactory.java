package com.nhl.link.move.runtime.json;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.QueryCompiler;

public class JsonExtractorFactory implements IExtractorFactory<StreamConnector> {

    public static final String JSON_QUERY_PROPERTY = "extractor.json.path";
    private static final String JSON_EXTRACTOR_TYPE = "json";

    private final IJacksonService jacksonService;
    private final QueryCompiler compiler;

    public JsonExtractorFactory() {
        this.jacksonService = new JacksonService();
        this.compiler = new QueryCompiler();
    }

    @Override
    public String getExtractorType() {
        return JSON_EXTRACTOR_TYPE;
    }

    @Override
    public Class<StreamConnector> getConnectorType() {
        return StreamConnector.class;
    }

    @Override
    public Extractor createExtractor(StreamConnector connector, ExtractorModel model) {
        String queryString = queryString(model);

        return new JsonExtractor(
                jacksonService,
                connector,
                mapToJsonAttributes(model.getAttributes()),
                queryString,
                compiler.compile(queryString));
    }

    private String queryString(ExtractorModel model) {
        String query = model.getPropertyValue(JSON_QUERY_PROPERTY);

        // TODO: should we just use "$" for root instead of throwing?
        if (query == null) {
            throw new IllegalArgumentException(String.format("Missing required property for key '%s'", JSON_QUERY_PROPERTY));
        }

        return query;
    }

    private JsonRowAttribute[] mapToJsonAttributes(RowAttribute[] attributes) {

        if (attributes == null) {
            return null;
        }

        int len = attributes.length;
        JsonRowAttribute[] jsonAttributes = new JsonRowAttribute[len];

        for (int i = 0; i < len; i++) {
            jsonAttributes[i] = new JsonRowAttribute(attributes[i], compiler);
        }
        return jsonAttributes;
    }
}
