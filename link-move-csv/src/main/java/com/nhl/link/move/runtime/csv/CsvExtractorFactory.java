package com.nhl.link.move.runtime.csv;

import java.nio.charset.Charset;

import com.nhl.link.move.runtime.extractor.IExtractorFactory;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import org.apache.commons.csv.CSVFormat;

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
     * @see java.nio.charset.Charset
     */
    public static final String CHARSET_PROPERTY = "extractor.csv.charset";

    private Charset defaultCharset;

    public CsvExtractorFactory() {
        defaultCharset = Charset.forName("UTF-8");
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
        try {

            String charsetName = model.getSingletonProperty(CHARSET_PROPERTY);

            Charset charset = charsetName != null ? Charset.forName(charsetName) : defaultCharset;

            CSVFormat csvFormat = CSVFormat.RFC4180;
            String delimiter = model.getSingletonProperty(DELIMITER_PROPERTY);
            if (delimiter != null) {
                if (delimiter.length() != 1) {
                    throw new LmRuntimeException("Invalid delimiter (should be exactly one character): " + delimiter);
                }
                csvFormat = csvFormat.withDelimiter(delimiter.charAt(0));
            }

            CsvExtractor extractor = new CsvExtractor(connector, model.getAttributes(), charset, csvFormat);

            String readFrom = model.getSingletonProperty(READ_FROM_PROPERTY);
            if (readFrom != null) {
                Integer n = Integer.valueOf(readFrom);
                extractor.setReadFrom(n);
            }

            return extractor;
        } catch (Exception e) {
            throw new LmRuntimeException("Failed to create extractor", e);
        }
    }

}
