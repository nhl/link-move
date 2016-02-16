package com.nhl.link.move.runtime.json;

import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.QueryCompiler;

public class JsonExtractorFactory implements IExtractorFactory<StreamConnector> {

	private static final String JSON_EXTRACTOR_TYPE = "json";
    public static final String JSON_QUERY_PROPERTY = "extractor.json.path";

    private IJacksonService jacksonService;
	private QueryCompiler compiler;

	public JsonExtractorFactory() {
        jacksonService = new JacksonService();
		compiler = new QueryCompiler();
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
        return new JsonExtractor(jacksonService, connector, model.getAttributes(), getJsonQuery(model));
	}

	private JsonQuery getJsonQuery(ExtractorModel model) {
		String query = model.getProperties().get(JSON_QUERY_PROPERTY);
		if (query == null) {
			throw new IllegalArgumentException(String.format(
                    "Missing required property for key '%s'", JSON_QUERY_PROPERTY));
		}
		return compiler.compile(query);
	}
}
