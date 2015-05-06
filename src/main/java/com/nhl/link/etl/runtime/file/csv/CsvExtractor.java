package com.nhl.link.etl.runtime.file.csv;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.RowAttribute;
import com.nhl.link.etl.RowReader;
import com.nhl.link.etl.extract.Extractor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * @since 1.4
 */
public class CsvExtractor implements Extractor {

    private File file;
    private RowAttribute[] attributes;
    private Charset charset;
    private String delimiter;
    private Integer readFrom;

    public CsvExtractor(File file, RowAttribute[] attributes) {
        this.file = file;
        this.attributes = attributes;
        this.charset = Charset.defaultCharset();
    }

    public CsvExtractor(File file, RowAttribute[] attributes, Charset charset) {
        this(file, attributes);
        this.charset = charset;
    }

    @Override
    public RowReader getReader(Map<String, ?> parameters) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file.toPath(), charset);
        } catch (IOException e) {
            throw new EtlRuntimeException("Failed to read lines from file: " + file.getPath(), e);
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
