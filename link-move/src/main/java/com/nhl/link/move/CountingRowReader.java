package com.nhl.link.move;

import java.util.Iterator;

/**
 * A {@link RowReader} decorator that counts each returned row using
 * {@link ExecutionStats}.
 *
 * @since 1.3
 */
public class CountingRowReader implements RowReader {

    private RowReader delegate;
    private ExecutionStats stats;

    public CountingRowReader(RowReader delegate, ExecutionStats stats) {
        this.delegate = delegate;
        this.stats = stats;
    }

    public void close() {
        delegate.close();
    }

    @Override
    public RowAttribute[] getHeader() {
        return delegate.getHeader();
    }

    public Iterator<Object[]> iterator() {
        Iterator<Object[]> it = delegate.iterator();

        return new Iterator<Object[]>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public void remove() {
                it.remove();
            }

            @Override
            public Object[] next() {
                stats.incrementExtracted(1);
                return it.next();
            }
        };
    }
}
