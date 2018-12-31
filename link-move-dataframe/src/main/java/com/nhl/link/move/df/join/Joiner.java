package com.nhl.link.move.df.join;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.ZippingDataFrame;
import com.nhl.link.move.df.filter.DataRowJoinPredicate;
import com.nhl.link.move.df.zip.Zipper;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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

        List<DataRow> lRows = new ArrayList<>();
        List<DataRow> rRows = new ArrayList<>();

        List<DataRow> allRRows = toList(rf);

        // O(N^2) performance

        for (DataRow lr : lf) {
            for (DataRow rr : allRRows) {
                if (joinPredicate.test(lr, rr)) {
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

        List<DataRow> allRRows = toList(rf);

        // O(N^2) performance

        for (DataRow lr : lf) {

            boolean hadMatches = false;
            for (DataRow rr : allRRows) {
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

        return new ZippingDataFrame(joinedColumns, lRows, rRows, Zipper::zipRows);
    }

    private DataFrame rightJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<DataRow> lRows = new ArrayList<>();
        List<DataRow> rRows = new ArrayList<>();

        List<DataRow> allLRows = toList(lf);

        // O(N^2) performance

        for (DataRow rr : rf) {

            boolean hadMatches = false;
            for (DataRow lr : allLRows) {
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

        return new ZippingDataFrame(joinedColumns, lRows, rRows, Zipper::zipRows);
    }

    private DataFrame fullJoin(Index joinedColumns, DataFrame lf, DataFrame rf) {

        List<DataRow> lRows = new ArrayList<>();
        Set<DataRow> rRows = new LinkedHashSet<>();

        List<DataRow> allRRows = toList(rf);
        Set<DataRow> seenRights = new LinkedHashSet<>();

        // O(N^2) performance

        for (DataRow lr : lf) {

            boolean hadMatches = false;


            for (DataRow rr : allRRows) {
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
        for (DataRow rr : allRRows) {
            if (!seenRights.contains(rr)) {
                lRows.add(null);
                rRows.add(rr);
            }
        }

        return new ZippingDataFrame(joinedColumns, lRows, rRows, Zipper::zipRows);
    }

    // "materialize" frame rows to avoid recalculation of each row on multiple iterations.

    // TODO: should we just add a caching wrapper around the DataFrame and assume elsewhere that DF iterator
    //  performance is decent?

    private List<DataRow> toList(DataFrame df) {
        List<DataRow> materialized = new ArrayList<>();
        df.forEach(materialized::add);
        return materialized;
    }
}
