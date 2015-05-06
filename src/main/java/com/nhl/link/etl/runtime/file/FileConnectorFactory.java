package com.nhl.link.etl.runtime.file;

import com.nhl.link.etl.connect.IConnectorFactory;

/**
 * @since 1.4
 */
public interface FileConnectorFactory extends IConnectorFactory<FileConnector> {

	@Override
	FileConnector createConnector(String id);
}
