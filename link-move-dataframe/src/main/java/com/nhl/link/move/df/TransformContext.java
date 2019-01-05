package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowToValueMapper;

public class TransformContext {

    private Index sourceIndex;
    private Index targetIndex;

    public TransformContext(Index sourceIndex, Index targetIndex) {
        this.sourceIndex = sourceIndex;
        this.targetIndex = targetIndex;
    }

    public Object read(Object[] sourceRow, String columnName) {
        return sourceIndex.position(columnName).read(sourceRow);
    }

    public Object read(Object[] sourceRow, int columnPos) {
        return sourceIndex.getPositions()[columnPos].read(sourceRow);
    }

    public Object[] target(Object... values) {

        if (values.length == targetIndex.size()) {
            return values;
        }

        if (values.length > targetIndex.size()) {
            throw new IllegalArgumentException("Provided values won't fit in the target row: "
                    + values.length + " > " + targetIndex.size());
        }

        Object[] target = new Object[targetIndex.size()];
        if (values.length > 0) {
            System.arraycopy(values, 0, target, 0, values.length);
        }

        return target;
    }

    public Object[] copyToTarget(Object[] sourceRow) {
        return copyToTarget(sourceRow, 0);
    }

    public <T> Object[] mapColumn(Object[] sourceRow, String name, DataRowToValueMapper<T> m) {
        return mapColumn(sourceRow, sourceIndex.position(name).ordinal(), m);
    }

    public <T> Object[] mapColumn(Object[] sourceRow, int sourcePos, DataRowToValueMapper<T> m) {
        Object[] target = copyToTarget(sourceRow);

        // since target is a compact version of the source, we can use "sourcePos" index directly on it
        target[sourcePos] = m.map(sourceRow);
        return target;
    }

    public Object[] copyToTarget(Object[] sourceRow, int targetOffset) {
        Object[] target = target();

        if (targetOffset > target.length) {
            return target;
        }

        System.arraycopy(sourceRow, 0, target, targetOffset, Math.min(target.length - targetOffset, sourceRow.length));
        return target;
    }
}
