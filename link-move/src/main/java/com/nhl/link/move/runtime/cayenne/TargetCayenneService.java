package com.nhl.link.move.runtime.cayenne;

import com.nhl.link.move.LmRuntimeException;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.access.DataNode;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.dba.DbAdapter;
import org.apache.cayenne.map.EntityResolver;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class TargetCayenneService implements ITargetCayenneService {

    private final ServerRuntime runtime;

    public TargetCayenneService(ServerRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public Map<String, DataSource> dataSources() {

        Map<String, DataSource> dataSources = new HashMap<>();

        for (DataNode n : runtime.getDataDomain().getDataNodes()) {

            // this method is used to seed a special kind of JdbcConnector.
            // But note that the DataSource here is attached to the DataNode
            // transaction, so reading source data will happen over the same
            // connection as writing target data. Hopefully such multiplexing
            // the connection works ok...

            dataSources.put(n.getName(), n.getDataSource());
        }

        return dataSources;
    }

    @Override
    public ObjectContext newContext() {
        return runtime.newContext();
    }

    @Override
    public EntityResolver entityResolver() {
        return runtime.getChannel().getEntityResolver();
    }

    @Override
    public DbAdapter dbAdapter(String dataMapName) {
        for (DataNode node : runtime.getDataDomain().getDataNodes()) {
            if (node.getDataMap(dataMapName) != null) {
                return node.getAdapter();
            }
        }

        throw new LmRuntimeException("Adapter not found for dataMap: " + dataMapName);
    }
}
