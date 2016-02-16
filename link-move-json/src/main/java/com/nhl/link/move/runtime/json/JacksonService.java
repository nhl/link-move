package com.nhl.link.move.runtime.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nhl.link.move.LmRuntimeException;

import java.io.IOException;
import java.io.InputStream;

public class JacksonService implements IJacksonService {

	private ObjectMapper sharedMapper;
	private JsonFactory sharedFactory;

	public JacksonService() {

		// fun Jackson API with circular dependencies ... so we create a mapper
		// first, and grab implicitly created factory from it
		this.sharedMapper = new ObjectMapper();
		this.sharedFactory = sharedMapper.getFactory();

		// make sure mapper does not attempt closing streams it does not
		// manage... why is this even a default in jackson?
		sharedFactory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

		// do not flush every time. why would we want to do that?
		// this is having a HUGE impact on extrest serializers (5x speedup)
		sharedMapper.disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
	}

	@Override
	public JsonFactory getJsonFactory() {
		return sharedFactory;
	}

	@Override
	public JsonNode parseJson(String json) {
		if (json == null) {
			return null;
		}

		try {
			JsonParser parser = getJsonFactory().createParser(json);
			return new ObjectMapper().readTree(parser);
		} catch (IOException ioex) {
			throw new LmRuntimeException("Error parsing JSON");
		}
	}

	@Override
	public JsonNode parseJson(InputStream json) {

		try {
			JsonParser parser = getJsonFactory().createParser(json);
			return new ObjectMapper().readTree(parser);
		} catch (IOException ioex) {
			throw new LmRuntimeException("Error parsing JSON");
		}
	}
}
