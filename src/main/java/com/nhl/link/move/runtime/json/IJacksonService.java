package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.InputStream;

public interface IJacksonService {

	JsonFactory getJsonFactory();
	JsonNode parseJson(String json);
	JsonNode parseJson(InputStream json);
}
