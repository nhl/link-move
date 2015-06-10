package com.nhl.link.etl.runtime.extractor;

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

import com.nhl.link.etl.EtlRuntimeException;
import com.nhl.link.etl.extractor.ExtractorConfig;
import com.nhl.link.etl.extractor.parser.DOMExtractorConfigParser;
import com.nhl.link.etl.extractor.parser.ExtractorConfigParser_1;
import com.nhl.link.etl.extractor.parser.VersionedExtractorConfigParser;

/**
 * Loads {@link ExtractorConfig} objects from XML streams. A decision on how an
 * XML stream is obtained is deferred to subclasses.
 */
public abstract class AbstractXmlExtractorConfigLoader implements IExtractorConfigLoader {

	private DocumentBuilderFactory domFactory;
	private DOMExtractorConfigParser parser;

	public AbstractXmlExtractorConfigLoader() {
		this.domFactory = DocumentBuilderFactory.newInstance();
		
		// important to have NS info available for schema versioning
		this.domFactory.setNamespaceAware(true);

		Map<String, DOMExtractorConfigParser> parsersByNS = new HashMap<>();
		parsersByNS.put(ExtractorConfigParser_1.NS, new ExtractorConfigParser_1());

		this.parser = new VersionedExtractorConfigParser(parsersByNS);
	}

	@Override
	public ExtractorConfig loadConfig(String name) {
		try (Reader in = getXmlSource(name);) {
			return processXml(name, in);
		} catch (IOException | ParserConfigurationException | SAXException | ClassNotFoundException | DOMException e) {
			throw new EtlRuntimeException("Error reading ExtractorConfig XML", e);
		}
	}

	protected abstract Reader getXmlSource(String name) throws IOException;

	@Override
	public abstract boolean needsReload(String name, long lastSeen);

	protected ExtractorConfig processXml(String name, Reader in) throws ParserConfigurationException, SAXException,
			IOException, ClassNotFoundException, DOMException {

		ExtractorConfig config = new ExtractorConfig(name);

		// don't expect large files, so using DOM for convenience

		DocumentBuilder domBuilder = domFactory.newDocumentBuilder();
		Element rootElement = domBuilder.parse(new InputSource(in)).getDocumentElement();
		parser.parse(rootElement, config);

		return config;
	}
}
