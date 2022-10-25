package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;


/**
 * @since 1.4
 */
public class CsvExtractor implements Extractor {

    private final StreamConnector connector;
    private final RowAttribute[] rowHeader;
    private final Charset charset;
    private final CSVFormat csvFormat;
    private final Integer readFrom;

    public CsvExtractor(
            StreamConnector connector,
            RowAttribute[] rowHeader,
            Charset charset,
            CSVFormat csvFormat,
            Integer readFrom) {

        this.connector = connector;
        this.rowHeader = rowHeader;
        this.charset = charset;
        this.csvFormat = csvFormat;
        this.readFrom = readFrom;
    }

    @Override
    public RowReader getReader(Execution exec) {

        CSVParser parser;
        try {
            Reader in = new InputStreamReader(connector.getInputStream(exec.getParameters()), charset);
            parser = csvFormat.parse(in);
        } catch (IOException e) {
            throw new LmRuntimeException("Failed to read CSV from stream", e);
        }

        CsvRowReader rowReader = new CsvRowReader(rowHeader, parser);
        if (readFrom != null) {
            rowReader.setReadFrom(readFrom);
        }

        exec.getLogger().extractorStarted(rowHeader, "");
        return rowReader;
    }
}
