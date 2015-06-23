package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.EtlRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @since 1.4
 */
public class CsvExtractor implements Extractor {

	private StreamConnector connector;
	private RowAttribute[] attributes;
	private Charset charset;
	private String delimiter;
	private Integer readFrom;

	public CsvExtractor(StreamConnector connector, RowAttribute[] attributes, Charset charset) {
		this.connector = connector;
		this.attributes = attributes;
		this.charset = charset;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {

		// TODO: this requires reading the entire file in memory. We can
		// probably turn this into a constant-memory streaming extractor that
		// reads one line at a time, processes and discards it.

		List<String> lines = new ArrayList<>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(connector.getInputStream(), charset));
			String line;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		} catch (IOException e) {
			throw new EtlRuntimeException("Failed to read lines from stream", e);
		}

		CsvRowReader reader = new CsvRowReader(attributes, lines);
		if (delimiter != null) {
			reader.setDelimiter(delimiter);
		}
		if (readFrom != null) {
			reader.setReadFrom(readFrom);
		}

		return reader;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public void setReadFrom(Integer readFrom) {
		this.readFrom = readFrom;
	}
}
