package com.nhl.link.move.df;

import com.nhl.link.move.df.map.DataRowMapper;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class TransformingDataFrameTest {

    @Test
    public void testIterator() {

        Index i = new Index("a", "b");
        List<Object[]> rows = asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2));

        TransformingDataFrame df = new TransformingDataFrame(i, rows, DataRowMapper.self());

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, "one", 1)
                .assertRow(1, "two", 2);
    }

    @Test
    public void testHead() {

        Index columns = new Index("a", "b");

        DataFrame df = new TransformingDataFrame(columns, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2),
                DataRow.row("three", 3)), DataRowMapper.self()).head(2);

        new DFAsserts(df, columns)
                .assertLength(2)
                .assertRow(0, "one", 1)
                .assertRow(1, "two", 2);
    }

    @Test
    public void testRenameColumn() {
        Index i = new Index("a", "b");

        DataFrame df = new TransformingDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2)), DataRowMapper.self()).renameColumn("b", "c");

        new DFAsserts(df, "a", "c")
                .assertLength(2)
                .assertRow(0, "one", 1)
                .assertRow(1, "two", 2);
    }

    @Test
    public void testMapColumn() {

        Index i = new Index("a", "b");

        DataFrame df = new TransformingDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2)), DataRowMapper.self()).mapColumn("b", r -> r[1].toString());

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, "one", "1")
                .assertRow(1, "two", "2");
    }

    @Test
    public void testMap() {

        Index i = new Index("a", "b");

        DataFrame df = new TransformingDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2)), DataRowMapper.self())
                .map(i, r -> DataRow.mapColumn(r, 0, v -> v[0] + "_"));

        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, "one_", 1)
                .assertRow(1, "two_", 2);
    }

    @Test
    public void testMap_ChangeRowStructure() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");

        DataFrame df = new TransformingDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2)), DataRowMapper.self())
                .map(i1, r -> DataRow.row(
                        r[0],
                        ((int) r[1]) * 10,
                        r[1]));

        new DFAsserts(df, i1)
                .assertLength(2)
                .assertRow(0, "one", 10, 1)
                .assertRow(1, "two", 20, 2);
    }

    @Test
    public void testMap_ChangeRowStructure_Chained() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");
        Index i2 = new Index("f", "g");

        DataFrame df = new TransformingDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2)), DataRowMapper.self())
                .map(i1, r -> DataRow.row(
                        r[0],
                        ((int) r[1]) * 10,
                        r[1]))
                .map(i2, r -> DataRow.row(
                        r[0],
                        r[1]));

        new DFAsserts(df, i2)
                .assertLength(2)
                .assertRow(0, "one", 10)
                .assertRow(1, "two", 20);
    }

    @Test
    public void testMap_ChangeRowStructure_EmptyDF() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");

        DataFrame df = new TransformingDataFrame(i, Collections.emptyList(), DataRowMapper.self())
                .map(i1, r -> DataRow.row(
                        r[0],
                        ((int) r[1]) * 10,
                        r[1]));

        assertSame(i1, df.getColumns());

        new DFAsserts(df, i1).assertLength(0);
    }

    @Test(expected = IllegalStateException.class)
    public void testMap_Index_Row_SizeMismatch() {

        Index i = new Index("a", "b");
        Index i1 = new Index("c", "d", "e");

        new TransformingDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2)), DataRowMapper.self())
                .map(i1, r -> DataRow.row(
                        r[0],
                        r[1]))
                // must throw when iterating due to inconsistent mapped row structure...
                .forEach(r -> {
                });
    }

    @Test
    public void testToString() {
        Index i = new Index("a", "b");
        DataFrame df = new TransformingDataFrame(i, asList(
                DataRow.row("one", 1),
                DataRow.row("two", 2),
                DataRow.row("three", 3),
                DataRow.row("four", 4)), DataRowMapper.self());

        assertEquals("LazyDataFrame [{a:one,b:1},{a:two,b:2},{a:three,b:3},...]", df.toString());
    }

    @Test
    public void testZip() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                DataRow.row(1),
                DataRow.row(2)), DataRowMapper.self());

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                DataRow.row(10),
                DataRow.row(20)), DataRowMapper.self());

        DataFrame df = df1.zip(df2);
        new DFAsserts(df, "a", "b")
                .assertLength(2)
                .assertRow(0, 1, 10)
                .assertRow(1, 2, 20);
    }

    @Test
    public void testZip_Self() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                DataRow.row(1),
                DataRow.row(2)), DataRowMapper.self());

        DataFrame df = df1.zip(df1);

        new DFAsserts(df, "a", "a_")
                .assertLength(2)
                .assertRow(0, 1, 1)
                .assertRow(1, 2, 2);
    }

    @Test
    public void testZip_LeftIsShorter() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, singletonList(
                DataRow.row(2)), DataRowMapper.self());

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                DataRow.row(10),
                DataRow.row(20)), DataRowMapper.self());

        DataFrame df = df1.zip(df2);
        new DFAsserts(df, "a", "b")
                .assertLength(1)
                .assertRow(0, 2, 10);
    }

    @Test
    public void testZip_RightIsShorter() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, singletonList(
                DataRow.row(2)), DataRowMapper.self());

        Index i2 = new Index("b");
        DataFrame df2 = new TransformingDataFrame(i2, asList(
                DataRow.row(10),
                DataRow.row(20)), DataRowMapper.self());

        DataFrame df = df2.zip(df1);
        new DFAsserts(df, "b", "a")
                .assertLength(1)
                .assertRow(0, 10, 2);
    }

    @Test
    public void testFilter() {

        Index i1 = new Index("a");
        DataFrame df1 = new TransformingDataFrame(i1, asList(
                DataRow.row(10),
                DataRow.row(20)), DataRowMapper.self());

        DataFrame df = df1.filter(r -> ((int) r[0]) > 15);
        new DFAsserts(df, "a")
                .assertLength(1)
                .assertRow(0, 20);
    }
}
