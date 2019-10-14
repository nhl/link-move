package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.runtime.extractor.IExtractorFactory;
import org.apache.cayenne.query.CapsStrategy;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;

public class JdbcExtractorFactory implements IExtractorFactory<JdbcConnector> {

	private static final String JDBC_EXTRACTOR_TYPE = "jdbc";
	public static final String SQL_TEMPLATE_PROPERTY = "extractor.jdbc.sqltemplate";

	/**
	 * Valid values are taken from Cayenne CapsStrategy - UPPER, LOWER, DEFAULT
	 */
	public static final String SQL_TEMPLATE_CAPS_PROPERTY = "extractor.jdbc.sqltemplate.caps";

	@Override
	public String getExtractorType() {
		return JDBC_EXTRACTOR_TYPE;
	}

	@Override
	public Class<JdbcConnector> getConnectorType() {
		return JdbcConnector.class;
	}

	@Override
	public Extractor createExtractor(JdbcConnector connector, ExtractorModel model) {

		String sqlTemplate = model.getSingletonProperty(SQL_TEMPLATE_PROPERTY);
		if (sqlTemplate == null) {
			throw new IllegalArgumentException("Missing required property for key '" + SQL_TEMPLATE_PROPERTY + "'");
		}

		// trim spaces coming from XML for cleaner output
		sqlTemplate = sqlTemplate.trim();

		CapsStrategy capsStrategy = CapsStrategy.DEFAULT;

		String capsStrategyString = model.getSingletonProperty(SQL_TEMPLATE_CAPS_PROPERTY);
		if (capsStrategyString != null) {
			capsStrategyString = capsStrategyString.trim().toUpperCase();
			capsStrategy = CapsStrategy.valueOf(capsStrategyString);
		}

		return new JdbcExtractor(connector.sharedContext(), model.getAttributes(), sqlTemplate, capsStrategy);
	}

}
