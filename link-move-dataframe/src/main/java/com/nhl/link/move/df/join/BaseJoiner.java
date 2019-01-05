package com.nhl.link.move.df.join;

import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.df.ZippingDataFrame;
import com.nhl.link.move.df.zip.Zipper;

public abstract class BaseJoiner {

    protected DataFrame zipJoinSides(Index joinedColumns, DataFrame lf, DataFrame rf) {
        return new ZippingDataFrame(joinedColumns, lf, rf, Zipper.rowZipper());
    }
}
