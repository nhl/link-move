package com.nhl.link.move.runtime.jdbc;

import com.nhl.link.move.BaseRowAttribute;
import com.nhl.link.move.RowAttribute;
import org.apache.cayenne.DataRow;
import org.apache.cayenne.ResultIterator;
import org.apache.cayenne.exp.parser.ASTDbPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An iterator over DataRows that can dynamically calculate row header from db data.
 */
public class DataRowIterator implements Iterator<DataRow>, AutoCloseable {

    private DataRow currentRow;
    private ResultIterator<DataRow> delegate;

    public DataRowIterator(ResultIterator<DataRow> delegate) {
        this.delegate = delegate;
        checkNextRow();
    }

    @Override
    public void close() {
        delegate.close();
    }

    public RowAttribute[] calculateHeader() {

        if (currentRow == null) {
            return new RowAttribute[0];
        }

        List<String> names = new ArrayList<>(currentRow.keySet());

        // ensure predictable order on each run...
        Collections.sort(names);

        RowAttribute[] header = new RowAttribute[currentRow.size()];
        for (int i = 0; i < header.length; i++) {
            String name = names.get(i);
            header[i] = new BaseRowAttribute(Object.class, name, ASTDbPath.DB_PREFIX + name, i);
        }

        return header;
    }

    @Override
    public boolean hasNext() {
        return currentRow != null;
    }

    @Override
    public DataRow next() {

        if (!hasNext()) {
            throw new ArrayIndexOutOfBoundsException("Past the end of the iterator");
        }

        DataRow row = currentRow;
        checkNextRow();
        return row;
    }

    private void checkNextRow() {
        this.currentRow = delegate.hasNextRow() ? delegate.nextRow() : null;
    }
}
