package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.extractor.Extractor;
import org.apache.cayenne.DataRow;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.query.CapsStrategy;
import org.apache.cayenne.query.SQLSelect;

import java.util.Map;

public class JdbcExtractor implements Extractor {

    private final ObjectContext context;
    private final String sqlTemplate;
    private final CapsStrategy capsStrategy;
    private final RowAttribute[] rowHeader;

    public JdbcExtractor(ObjectContext context, RowAttribute[] rowHeader, String sqlTemplate,
                         CapsStrategy capsStrategy) {
        this.context = context;
        this.sqlTemplate = sqlTemplate;
        this.capsStrategy = capsStrategy;
        this.rowHeader = rowHeader;
    }

    @Override
    public RowReader getReader(Map<String, ?> parameters) {

        // TODO: fetching DataRows and then converting them to Object[] is kind of expensive...
        // maybe we can create Object[] bypassing DR, ideally by iterating a JDBC ResultSet

        SQLSelect<DataRow> select = SQLSelect
                .dataRowQuery(sqlTemplate)
                .params(parameters);

        switch (capsStrategy) {
            case LOWER:
                select.lowerColumnNames();
                break;
            case UPPER:
            case DEFAULT:
                select.upperColumnNames();
                break;
        }

        DataRowIterator it = new DataRowIterator(context.iterator(select));
        RowAttribute[] rowHeader = this.rowHeader != null ? this.rowHeader : it.calculateHeader();
        return new JdbcRowReader(rowHeader, it);
    }
}
