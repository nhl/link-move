package com.nhl.link.move.df.join;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.ZippingDataFrame;
import com.nhl.link.move.df.map.DataRowToValueMapper;
import com.nhl.link.move.df.zip.Zipper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A DataFrame joiner based on a pair of functions that can calculate compareable keys for the left and right row.
 * Should theoretically have O(N + M) performance.
 */
public class IndexedJoiner<K> {

    private DataRowToValueMapper<K> leftKeyMapper;
    private DataRowToValueMapper<K> rightKeyMapper;
    private JoinSemantics semantics;

    public IndexedJoiner(
            DataRowToValueMapper<K> leftKeyMapper,
            DataRowToValueMapper<K> rightKeyMapper,
            JoinSemantics semantics) {

        this.leftKeyMapper = leftKeyMapper;
        this.rightKeyMapper = rightKeyMapper;
        this.semantics = semantics;
    }

    public Index joinIndex(Index li, Index ri) {
        return Zipper.zipIndex(li, ri);
    }

    public DataFrame joinRows(Index joinedColumns, DataFrame lf, DataFrame rf) {
        switch (semantics) {
            case inner:
                return innerJoin(joinedColumns, lf, rf);
            case left:
                return leftJoin(joinedColumns, lf, rf);
            case right:
                return rightJoin(joinedColumns, lf, rf);
            case full:
                return fullJoin(joinedColumns, lf, rf);
            default:
                throw new IllegalStateException("Unsupported join semantics: " + semantics);
        }
    }

    private DataFrame innerJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<DataRow> lRows = new ArrayList<>();
        List<DataRow> rRows = new ArrayList<>();

        Map<K, List<DataRow>> rightIndex = groupByKey(rightKeyMapper, rf);

        for (DataRow lr : lf) {

            K lKey = leftKeyMapper.map(lr);
            List<DataRow> rightMatches = rightIndex.get(lKey);
            if (rightMatches != null) {
                for (DataRow rr : rightMatches) {
                    lRows.add(lr);
                    rRows.add(rr);
                }
            }
        }

        return new ZippingDataFrame(joinedColumns, lRows, rRows, Zipper::zipRows);
    }

    private DataFrame leftJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<DataRow> lRows = new ArrayList<>();
        List<DataRow> rRows = new ArrayList<>();

        Map<K, List<DataRow>> rightIndex = groupByKey(rightKeyMapper, rf);

        for (DataRow lr : lf) {

            K lKey = leftKeyMapper.map(lr);
            List<DataRow> rightMatches = rightIndex.get(lKey);

            if (rightMatches != null) {
                for (DataRow rr : rightMatches) {
                    lRows.add(lr);
                    rRows.add(rr);
                }
            } else {
                lRows.add(lr);
                rRows.add(null);
            }
        }

        return new ZippingDataFrame(joinedColumns, lRows, rRows, Zipper::zipRows);
    }

    private DataFrame rightJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<DataRow> lRows = new ArrayList<>();
        List<DataRow> rRows = new ArrayList<>();

        Map<K, List<DataRow>> leftIndex = groupByKey(leftKeyMapper, lf);

        for (DataRow rr : rf) {

            K rKey = rightKeyMapper.map(rr);
            List<DataRow> leftMatches = leftIndex.get(rKey);

            if (leftMatches != null) {
                for (DataRow lr : leftMatches) {
                    lRows.add(lr);
                    rRows.add(rr);
                }
            } else {
                lRows.add(null);
                rRows.add(rr);
            }
        }

        return new ZippingDataFrame(joinedColumns, lRows, rRows, Zipper::zipRows);
    }

    private DataFrame fullJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<DataRow> lRows = new ArrayList<>();
        Set<DataRow> rRows = new LinkedHashSet<>();

        Map<K, List<DataRow>> rightIndex = groupByKey(rightKeyMapper, rf);
        Set<DataRow> seenRights = new LinkedHashSet<>();

        for (DataRow lr : lf) {

            K lKey = leftKeyMapper.map(lr);
            List<DataRow> rightMatches = rightIndex.get(lKey);

            if (rightMatches != null) {
                for (DataRow rr : rightMatches) {
                    lRows.add(lr);
                    rRows.add(rr);
                    seenRights.add(rr);
                }
            } else {
                lRows.add(lr);
                rRows.add(null);
            }
        }

        // add missing right rows
        for (List<DataRow> rrl : rightIndex.values()) {
            for (DataRow rr : rrl) {
                if (!seenRights.contains(rr)) {
                    lRows.add(null);
                    rRows.add(rr);
                }
            }
        }

        return new ZippingDataFrame(joinedColumns, lRows, rRows, Zipper::zipRows);
    }


    private Map<K, List<DataRow>> groupByKey(DataRowToValueMapper<K> keyMapper, DataFrame df) {

        Map<K, List<DataRow>> index = new HashMap<>();

        for (DataRow r : df) {
            K key = keyMapper.map(r);
            index.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
        }

        return index;
    }
}
