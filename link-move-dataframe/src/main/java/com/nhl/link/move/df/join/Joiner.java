package com.nhl.link.move.df.join;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.ZippingDataFrame;
import com.nhl.link.move.df.filter.DataRowJoinPredicate;
import com.nhl.link.move.df.zip.Zipper;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A DataFrame joiner based on rows comparing predicate. Should theoretically have O(N * M) performance.
 */
public class Joiner {

    private DataRowJoinPredicate joinPredicate;
    private JoinSemantics semantics;

    public Joiner(DataRowJoinPredicate joinPredicate, JoinSemantics semantics) {
        this.joinPredicate = Objects.requireNonNull(joinPredicate);
        this.semantics = Objects.requireNonNull(semantics);
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

        List<Object[]> lRows = new ArrayList<>();
        List<Object[]> rRows = new ArrayList<>();

        List<Object[]> allRRows = toList(rf);

        for (Object[] lr : lf) {
            for (Object[] rr : allRRows) {
                if (joinPredicate.test(lr, rr)) {
                    lRows.add(lr);
                    rRows.add(rr);
                }
            }
        }

        return toJoinDataFrame(joinedColumns, lf.getColumns(), rf.getColumns(), lRows, rRows);
    }

    private DataFrame leftJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<Object[]> lRows = new ArrayList<>();
        List<Object[]> rRows = new ArrayList<>();

        List<Object[]> allRRows = toList(rf);

        for (Object[] lr : lf) {

            boolean hadMatches = false;
            for (Object[] rr : allRRows) {
                if (joinPredicate.test(lr, rr)) {
                    lRows.add(lr);
                    rRows.add(rr);
                    hadMatches = true;
                }
            }

            if (!hadMatches) {
                lRows.add(lr);
                rRows.add(null);
            }
        }

        return toJoinDataFrame(joinedColumns, lf.getColumns(), rf.getColumns(), lRows, rRows);
    }

    private DataFrame rightJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<Object[]> lRows = new ArrayList<>();
        List<Object[]> rRows = new ArrayList<>();

        List<Object[]> allLRows = toList(lf);

        for (Object[] rr : rf) {

            boolean hadMatches = false;
            for (Object[] lr : allLRows) {
                if (joinPredicate.test(lr, rr)) {
                    lRows.add(lr);
                    rRows.add(rr);
                    hadMatches = true;
                }
            }

            if (!hadMatches) {
                lRows.add(null);
                rRows.add(rr);
            }
        }

        return toJoinDataFrame(joinedColumns, lf.getColumns(), rf.getColumns(), lRows, rRows);
    }

    private DataFrame fullJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<Object[]> lRows = new ArrayList<>();
        Set<Object[]> rRows = new LinkedHashSet<>();

        List<Object[]> allRRows = toList(rf);
        Set<Object[]> seenRights = new LinkedHashSet<>();

        for (Object[] lr : lf) {

            boolean hadMatches = false;

            for (Object[] rr : allRRows) {
                if (joinPredicate.test(lr, rr)) {
                    lRows.add(lr);
                    rRows.add(rr);
                    hadMatches = true;
                    seenRights.add(rr);
                }
            }

            if (!hadMatches) {
                lRows.add(lr);
                rRows.add(null);
            }
        }

        // add missing right rows
        for (Object[] rr : allRRows) {
            if (!seenRights.contains(rr)) {
                lRows.add(null);
                rRows.add(rr);
            }
        }

        return toJoinDataFrame(joinedColumns, lf.getColumns(), rf.getColumns(), lRows, rRows);
    }

    private DataFrame toJoinDataFrame(Index joinedColumns, Index lin, Index rin, Iterable<Object[]> li, Iterable<Object[]> ri) {
        return new ZippingDataFrame(joinedColumns, li, ri, Zipper.rowZipper(lin, rin));
    }

    // "materialize" frame rows to avoid recalculation of each row on multiple iterations.

    // TODO: should we just add a caching / materialization wrapper around the DataFrame and assume elsewhere that DF
    //  iterator performance is decent?

    private List<Object[]> toList(DataFrame df) {
        List<Object[]> materialized = new ArrayList<>();
        df.forEach(materialized::add);
        return materialized;
    }
}
