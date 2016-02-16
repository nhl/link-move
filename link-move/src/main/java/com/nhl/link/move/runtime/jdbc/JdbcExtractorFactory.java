package com.nhl.link.move.runtime.jdbc;

import org.apache.cayenne.di.Inject;
import org.apache.cayenne.query.CapsStrategy;

import com.nhl.link.move.extractor.Extractor;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.runtime.connect.IConnectorService;
import com.nhl.link.move.runtime.extractor.BaseExtractorFactory;

public class JdbcExtractorFactory extends BaseExtractorFactory<JdbcConnector> {

	public static final String SQL_TEMPLATE_PROPERTY = "extractor.jdbc.sqltemplate";

	/**
	 * Valid values are taken from Cayenne CapsStrategy - UPPER, LOWER, DEFAULT
	 */
	public static final String SQL_TEMPLATE_CAPS_PROPERTY = "extractor.jdbc.sqltemplate.caps";

	public JdbcExtractorFactory(@Inject IConnectorService connectorService) {
		super(connectorService);
	}

	@Override
	protected Class<JdbcConnector> getConnectorType() {
		return JdbcConnector.class;
	}

	@Override
	protected Extractor createExtractor(JdbcConnector connector, ExtractorModel model) {

		String sqlTemplate = model.getProperties().get(SQL_TEMPLATE_PROPERTY);
		if (sqlTemplate == null) {
			throw new IllegalArgumentException("Missing required property for key '" + SQL_TEMPLATE_PROPERTY + "'");
		}

		// trim spaces coming from XML for cleaner output
		sqlTemplate = sqlTemplate.trim();

		CapsStrategy capsStrategy = CapsStrategy.DEFAULT;

		String capsStrategyString = model.getProperties().get(SQL_TEMPLATE_CAPS_PROPERTY);
		if (capsStrategyString != null) {
			capsStrategyString = capsStrategyString.trim().toUpperCase();
			capsStrategy = CapsStrategy.valueOf(capsStrategyString);
		}

		return new JdbcExtractor(connector.sharedContext(), model.getAttributes(), sqlTemplate, capsStrategy);
	}

}
