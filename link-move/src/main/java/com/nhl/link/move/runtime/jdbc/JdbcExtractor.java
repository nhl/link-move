package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.Execution;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import com.nhl.link.move.extractor.Extractor;
import org.apache.cayenne.DataRow;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.query.CapsStrategy;
import org.apache.cayenne.query.SQLSelect;

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
    public RowReader getReader(Execution exec) {

        //  Fetching DataRows and then converting them to Object[], and then to a columnar DataFrame is kind of expensive.
        //  We could use "columnQuery" instead of "dataRowQuery" to cut one step, but it won't provide us with column
        //  names in case our model is missing an explicit "rowHeader"

        // TODO: this should use DFLib directly for maximum performance

        SQLSelect<DataRow> select = SQLSelect.dataRowQuery(sqlTemplate).params(exec.getParameters());

        switch (capsStrategy) {
            case LOWER:
                select.lowerColumnNames();
                break;
            case UPPER:
                select.upperColumnNames();
                break;
            case DEFAULT:
                break;
        }

        DataRowIterator it = new DataRowIterator(context.iterator(select));
        RowAttribute[] rowHeader = this.rowHeader != null ? this.rowHeader : it.calculateHeader();

        exec.getLogger().extractorStarted(rowHeader, sqlTemplate);
        return new JdbcRowReader(rowHeader, it);
    }
}
