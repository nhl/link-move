package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;


/**
 * @since 1.4
 */
public class CsvExtractor implements Extractor {

	private StreamConnector connector;
	private RowAttribute[] rowHeader;
	private Charset charset;
	private CSVFormat csvFormat;

	private Integer readFrom;

	public CsvExtractor(StreamConnector connector, RowAttribute[] rowHeader, Charset charset) {
		this(connector, rowHeader, charset, CSVFormat.DEFAULT);
	}

	public CsvExtractor(StreamConnector connector, RowAttribute[] rowHeader, Charset charset, CSVFormat csvFormat) {
		this.connector = connector;
		this.rowHeader = rowHeader;
		this.charset = charset;
		this.csvFormat = csvFormat;
	}

	@Override
	public RowReader getReader(Map<String, ?> parameters) {
		CSVParser parser;
		try {
			parser = csvFormat.parse(new InputStreamReader(connector.getInputStream(), charset));
		} catch (IOException e) {
			throw new LmRuntimeException("Failed to read CSV from stream", e);
		}
		CsvRowReader rowReader = new CsvRowReader(rowHeader, parser);
		if (readFrom != null) {
			rowReader.setReadFrom(readFrom);
		}
		return rowReader;
	}

	public void setReadFrom(Integer readFrom) {
		this.readFrom = readFrom;
	}
}
