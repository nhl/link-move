package com.nhl.link.move.df;

import com.nhl.link.move.df.map.ValueMapper;

public interface DataRow {

    /**
     * Syntactic sugar for array construction. Equivalent to "new Object[] {x, y, z}".
     *
     * @param values a varargs array of values
     * @return "values" vararg unchanged
     */
    static Object[] row(Object... values) {
        return values;
    }

    static <VR> Object[] mapColumn(Object[] row, int position, ValueMapper<Object[], VR> m) {

        int width = row.length;

        Object[] newValues = new Object[width];
        System.arraycopy(row, 0, newValues, 0, width);
        newValues[position] = m.map(row);

        return newValues;
    }
}
