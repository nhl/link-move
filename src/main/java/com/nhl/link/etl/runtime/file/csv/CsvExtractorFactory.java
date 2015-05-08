package com.nhl.link.etl.runtime.file.csv;

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.connect.StreamConnector;
import com.nhl.link.etl.extract.Extractor;
import com.nhl.link.etl.extract.ExtractorConfig;
import com.nhl.link.etl.runtime.connect.IConnectorService;
import com.nhl.link.etl.runtime.extract.BaseExtractorFactory;
import org.apache.cayenne.di.Inject;

import java.nio.charset.Charset;

/**
 * @since 1.4
 */
public class CsvExtractorFactory extends BaseExtractorFactory<StreamConnector> {

    /**
     * Delimiter character.
     */
    public static final String DELIMITIER_PROPERTY = "extractor.csv.delimiter";

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

    public CsvExtractorFactory(@Inject IConnectorService connectorService) {
        super(connectorService);
    }

    @Override
    protected Class<StreamConnector> getConnectorType() {
        return StreamConnector.class;
    }

    @Override
    protected Extractor createExtractor(StreamConnector connector, ExtractorConfig config) {
        try {
            CsvExtractor extractor;

            String charsetName = config.getProperties().get(CHARSET_PROPERTY);
            if (charsetName == null) {
                extractor = new CsvExtractor(connector, config.getAttributes());
            } else {
                extractor = new CsvExtractor(connector, config.getAttributes(), Charset.forName(charsetName));
            }

            String delimiter = config.getProperties().get(DELIMITIER_PROPERTY);
            if (delimiter != null) {
                extractor.setDelimiter(delimiter);
            }

            String readFrom = config.getProperties().get(READ_FROM_PROPERTY);
            if (readFrom != null) {
                Integer n = Integer.valueOf(readFrom);
                extractor.setReadFrom(n);
            }

            return extractor;
        } catch (Exception e) {
            throw new EtlRuntimeException("Failed to create extractor", e);
        }
    }

}
