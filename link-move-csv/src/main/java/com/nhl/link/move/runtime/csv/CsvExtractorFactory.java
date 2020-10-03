package com.nhl.link.move.runtime.csv;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import org.apache.commons.csv.CSVFormat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @since 1.4
 */
public class CsvExtractorFactory implements IExtractorFactory<StreamConnector> {

    private static final String CSV_EXTRACTOR_TYPE = "csv";

    /**
     * Delimiter character.
     */
    public static final String DELIMITER_PROPERTY = "extractor.csv.delimiter";

    /**
     * Line number, that the file should be read from.
     * Valid values are positive integers (starting with 1).
     */
    public static final String READ_FROM_PROPERTY = "extractor.csv.readFrom";

    /**
     * Source file charset name.
     *
     * @see java.nio.charset.Charset
     */
    public static final String CHARSET_PROPERTY = "extractor.csv.charset";

    private final Charset defaultCharset;

    public CsvExtractorFactory() {
        defaultCharset = StandardCharsets.UTF_8;
    }

    @Override
    public String getExtractorType() {
        return CSV_EXTRACTOR_TYPE;
    }

    @Override
    public Class<StreamConnector> getConnectorType() {
        return StreamConnector.class;
    }

    @Override
    public Extractor createExtractor(StreamConnector connector, ExtractorModel model) {

        String charsetName = model.getPropertyValue(CHARSET_PROPERTY);

        Charset charset = charsetName != null ? Charset.forName(charsetName) : defaultCharset;

        CSVFormat csvFormat = CSVFormat.RFC4180;
        String delimiter = model.getPropertyValue(DELIMITER_PROPERTY);
        if (delimiter != null) {
            if (delimiter.length() != 1) {
                throw new LmRuntimeException("Invalid delimiter (should be exactly one character): " + delimiter);
            }
            csvFormat = csvFormat.withDelimiter(delimiter.charAt(0));
        }

        String readFromString = model.getPropertyValue(READ_FROM_PROPERTY);
        Integer readFrom = readFromString != null ? Integer.valueOf(readFromString) : null;

        return new CsvExtractor(connector, model.getAttributes(), charset, csvFormat, readFrom);
    }

}
