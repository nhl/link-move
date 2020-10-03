package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.BaseRowAttribute;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XmlRowReaderTest {

	private static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<tests>"
			+ "  <test><k1>v11</k1><k2>v12</k2></test>  <test><k1>v21</k1><k2>v22</k2></test>"
			+ "  <test><k1>v31</k1><k2>v32</k2></test></tests>";

	private XPathFactory xPathFactory;
	private XmlRowReader xmlRowReader;
	private XmlRowAttribute[] attributes;
	private NodeList nodes;

	@BeforeEach
	public void setUpXmlRowReader() throws Exception {
		xPathFactory = XPathFactory.newInstance();
		configAttributes();
		configNodes();
		xmlRowReader = new XmlRowReader(attributes, nodes);
	}

	private void configAttributes() {
		attributes = new XmlRowAttribute[2];
		attributes[0] = new XmlRowAttribute(new BaseRowAttribute(String.class, "k1", "k1", 0), xPathFactory);
		attributes[1] = new XmlRowAttribute(new BaseRowAttribute(String.class, "k2", "k1", 0), xPathFactory);
	}

	private void configNodes() throws Exception {
		XPath xPath = XPathFactory.newInstance().newXPath();
		nodes = (NodeList) xPath
				.evaluate("/tests/test", new InputSource(new StringReader(XML)), XPathConstants.NODESET);
	}

	@Test
	public void testIterator() {
		Iterator<Object[]> rowIterator = xmlRowReader.iterator();
		int i = 0;

		while (rowIterator.hasNext()) {
			i++;
			Object[] row = rowIterator.next();
			assertEquals("v" + i + "1", row[0]);
			assertEquals("v" + i + "2", row[1]);
		}

		assertEquals(3, i);
	}
}