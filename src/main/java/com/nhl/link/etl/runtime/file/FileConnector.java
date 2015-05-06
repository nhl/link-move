package com.nhl.link.etl.runtime.file;

import com.nhl.link.etl.connect.Connector;

import java.io.File;
import java.net.URI;

public class FileConnector implements Connector {

    private File file;

    public FileConnector(URI uri) {
        file = new File(uri);
    }

    public File getFile() {
        return file;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

}
