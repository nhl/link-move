package com.nhl.link.move.runtime.json;

import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.BaseExtractorFactory;
import com.nhl.link.move.runtime.json.query.JsonQuery;
import com.nhl.link.move.runtime.json.query.QueryCompiler;
import org.apache.cayenne.di.Inject;

public class JsonExtractorFactory extends BaseExtractorFactory<StreamConnector> {

    public static final String JSON_QUERY_PROPERTY = "extractor.json.path";

    private IJacksonService jacksonService;
	private QueryCompiler compiler;

	public JsonExtractorFactory(@Inject IConnectorService connectorService, @Inject IJacksonService jacksonService) {
		super(connectorService);
        this.jacksonService = jacksonService;
		compiler = new QueryCompiler();
	}

	@Override
	protected Class<StreamConnector> getConnectorType() {
		return StreamConnector.class;
	}

	@Override
	protected Extractor createExtractor(StreamConnector connector, ExtractorModel model) {
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
