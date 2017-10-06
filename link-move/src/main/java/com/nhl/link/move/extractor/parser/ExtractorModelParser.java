package com.nhl.link.move.extractor.parser;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 2.4
 */
public class ExtractorModelParser implements IExtractorModelParser {

    // use v1 namespace as the default
    static final String NO_NS_PARSER_NS = ExtractorModelParser_v1.NS;

    private DocumentBuilderFactory domFactory;
    private DOMExtractorModelParser parser;

    public ExtractorModelParser() {
        this.domFactory = DocumentBuilderFactory.newInstance();

        // important to have NS info available for schema versioning
        this.domFactory.setNamespaceAware(true);

        Map<String, DOMExtractorModelParser> parsersByNS = new HashMap<>();
        parsersByNS.put(ExtractorModelParser_v1.NS, new ExtractorModelParser_v1());
        parsersByNS.put(ExtractorModelParser_v2.NS, new ExtractorModelParser_v2());

        this.parser = new VersionedExtractorModelParser(parsersByNS, NO_NS_PARSER_NS);
    }


    public ExtractorModelContainer parse(String name, Reader in) {
        try {
            return parseWithExceptions(name, in);
        } catch (IOException | ParserConfigurationException | SAXException | ClassNotFoundException | DOMException e) {
            throw new LmRuntimeException("Error reading ExtractorConfig XML", e);
        }
    }

    protected ExtractorModelContainer parseWithExceptions(String name, Reader in) throws
            ParserConfigurationException,
            SAXException,
            IOException,
            ClassNotFoundException,
            DOMException {

        // don't expect large files, so using DOM for convenience

        DocumentBuilder domBuilder = domFactory.newDocumentBuilder();
        Element xmlRoot = domBuilder.parse(new InputSource(in)).getDocumentElement();
        return parser.parse(name, xmlRoot);
    }
}
