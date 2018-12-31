package com.nhl.link.move.df;

import com.nhl.link.move.df.map.ValueMapper;

public interface DataRow {

    /**
     * Syntactic sugar for array construction. Equivalent to "new Object[] {x, y, z}".
     *
     * @param values a varargs array of values
     * @return "values" vararg unchanged
     */
    static Object[] values(Object... values) {
        return values;
    }

    Object get(String columnName);

    Object get(int position);

    Index getIndex();

    DataRow reindex(Index columns);

    <V, VR> Object[] mapColumn(int position, ValueMapper<V, VR> m);

    void copyTo(Object[] to, int toOffset);

    default Object[] copyValues() {
        Object[] values = new Object[size()];
        copyTo(values, 0);
        return values;
    }

    default int size() {
        return getIndex().size();
    }
}
