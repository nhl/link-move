package com.nhl.link.framework.etl.runtime.extract;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.junit.Test;

import com.nhl.link.framework.etl.extract.ExtractorConfig;

public class AbstractXmlExtractorConfigLoaderTest {

	@Test
	public void testLoadConfig() {

		AbstractXmlExtractorConfigLoader loader = new AbstractXmlExtractorConfigLoader() {

			@Override
			protected Reader getXmlSource(String name) throws IOException {
				return new StringReader(
						"<config>"
								+ "  <type>atype</type>"
								+ "  <connectorId>aconnector</connectorId>"
								+ "  <attributes>"
								+ "    <attribute><type>java.lang.String</type><source>a1</source><target>a_1</target></attribute>"
								+ "    <attribute><type>java.lang.Integer</type><source>a2</source><target>a_2</target></attribute>"
								+ "    <attribute><type>java.lang.Integer</type><source>a2</source></attribute>"
								+ "  </attributes>" + "  <properties>" + "    <a.b>AB</a.b>" + "    <x.y>XY</x.y>"
								+ "  </properties>" + "</config>");
			}

			@Override
			public boolean needsReload(String name, long lastSeen) {
				return true;
			}
		};

		ExtractorConfig config = loader.loadConfig("aname");
		assertNotNull(config);
		assertEquals("aname", config.getName());
		assertEquals("atype", config.getType());
		assertEquals("aconnector", config.getConnectorId());
		assertEquals(3, config.getAttributes().length);

		assertEquals(0, config.getAttributes()[0].ordinal());
		assertEquals(String.class, config.getAttributes()[0].type());
		assertEquals("a1", config.getAttributes()[0].sourceName());
		assertEquals("a_1", config.getAttributes()[0].targetName());

		assertEquals(1, config.getAttributes()[1].ordinal());
		assertEquals(Integer.class, config.getAttributes()[1].type());
		assertEquals("a2", config.getAttributes()[1].sourceName());
		assertEquals("a_2", config.getAttributes()[1].targetName());

		assertEquals(2, config.getAttributes()[2].ordinal());
		assertEquals(Integer.class, config.getAttributes()[2].type());
		assertEquals("a2", config.getAttributes()[2].sourceName());
		assertEquals("a2", config.getAttributes()[2].targetName());

		assertEquals(2, config.getProperties().size());
		assertEquals("AB", config.getProperties().get("a.b"));
		assertEquals("XY", config.getProperties().get("x.y"));
	}

	@Test
	public void testGetType() throws ClassNotFoundException {
		AbstractXmlExtractorConfigLoader loader = new AbstractXmlExtractorConfigLoader() {
			@Override
			protected Reader getXmlSource(String name) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean needsReload(String name, long lastSeen) {
				throw new UnsupportedOperationException();
			}
		};

		assertSame(String.class, loader.getType("java.lang.String"));
		assertSame(Integer.class, loader.getType("java.lang.Integer"));
		assertSame(byte[].class, loader.getType("byte[]"));
		assertSame(Integer.TYPE, loader.getType("int"));
	}
}
