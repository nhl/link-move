package com.nhl.link.etl.runtime.file;

import com.nhl.link.etl.connect.IConnectorFactory;

public interface FileConnectorFactory extends IConnectorFactory<FileConnector> {

    public FileConnector createConnector(String id);

}
