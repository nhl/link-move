package com.nhl.link.move.runtime.csv;

import java.nio.charset.Charset;

import org.apache.cayenne.di.Inject;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.connect.StreamConnector;
import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.BaseExtractorFactory;

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
    protected Extractor createExtractor(StreamConnector connector, ExtractorModel model) {
        try {

            String charsetName = model.getProperties().get(CHARSET_PROPERTY);
            
            // TODO: should we lock default Charset to UTF-8 instead of platform-default?
            Charset charset = charsetName != null ? Charset.forName(charsetName) : Charset.defaultCharset();
            
            CsvExtractor extractor = new CsvExtractor(connector, model.getAttributes(), charset);

            String delimiter = model.getProperties().get(DELIMITIER_PROPERTY);
            if (delimiter != null) {
                extractor.setDelimiter(delimiter);
            }

            String readFrom = model.getProperties().get(READ_FROM_PROPERTY);
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
