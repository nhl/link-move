package com.nhl.link.move.runtime.extractor.model;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.extractor.parser.DOMExtractorModelParser;
import com.nhl.link.move.extractor.parser.ExtractorModelParser_v1;
import com.nhl.link.move.extractor.parser.ExtractorModelParser_v2;
import com.nhl.link.move.extractor.parser.VersionedExtractorModelParser;

/**
 * Loads {@link ExtractorModelContainer} objects from XML streams. A decision on
 * how an XML stream is obtained is deferred to subclasses.
 */
public abstract class BaseExtractorModelLoader implements IExtractorModelLoader {

	// use v1 namespace as the default
	static final String NO_NS_PARSER_NS = ExtractorModelParser_v1.NS;

	private DocumentBuilderFactory domFactory;
	private DOMExtractorModelParser parser;

	public BaseExtractorModelLoader() {
		this.domFactory = DocumentBuilderFactory.newInstance();

		// important to have NS info available for schema versioning
		this.domFactory.setNamespaceAware(true);

		Map<String, DOMExtractorModelParser> parsersByNS = new HashMap<>();
		parsersByNS.put(ExtractorModelParser_v1.NS, new ExtractorModelParser_v1());
		parsersByNS.put(ExtractorModelParser_v2.NS, new ExtractorModelParser_v2());

		this.parser = new VersionedExtractorModelParser(parsersByNS, NO_NS_PARSER_NS);
	}

	@Override
	public ExtractorModelContainer load(String name) {
		try (Reader in = getXmlSource(name);) {
			return processXml(name, in);
		} catch (IOException | ParserConfigurationException | SAXException | ClassNotFoundException | DOMException e) {
			throw new LmRuntimeException("Error reading ExtractorConfig XML", e);
		}
	}

	protected abstract Reader getXmlSource(String name) throws IOException;

	@Override
	public abstract boolean needsReload(ExtractorModelContainer container);

	protected ExtractorModelContainer processXml(String name, Reader in) throws ParserConfigurationException,
			SAXException, IOException, ClassNotFoundException, DOMException {

		// don't expect large files, so using DOM for convenience

		DocumentBuilder domBuilder = domFactory.newDocumentBuilder();
		Element xmlRoot = domBuilder.parse(new InputSource(in)).getDocumentElement();
		return parser.parse(name, xmlRoot);
	}
}
