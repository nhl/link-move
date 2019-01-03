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

    static Object[] copy(Object[] row) {
        int len = row.length;
        Object[] copy = new Object[len];
        System.arraycopy(row, 0, copy, 0, len);
        return copy;
    }

    static Object[] copyTo(Object[] row, Object[] to, int toOffset) {
        System.arraycopy(row, 0, to, toOffset, row.length);
        return to;
    }

    static <VR> Object[] addColumn(Object[] row, ValueMapper<Object[], VR> m) {

        int oldWidth = row.length;

        Object[] newValues = new Object[oldWidth + 1];
        System.arraycopy(row, 0, newValues, 0, oldWidth);
        newValues[oldWidth] = m.map(row);

        return newValues;
    }

    static <VR> Object[] mapColumn(Object[] row, int position, ValueMapper<Object[], VR> m) {

        int width = row.length;

        Object[] newValues = new Object[width];
        System.arraycopy(row, 0, newValues, 0, width);
        newValues[position] = m.map(row);

        return newValues;
    }

}
