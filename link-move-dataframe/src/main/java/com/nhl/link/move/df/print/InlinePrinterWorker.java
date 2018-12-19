package com.nhl.link.move.df.print;

import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;

import java.util.Iterator;

public class InlinePrinterWorker extends BasePrinterWorker {

    public InlinePrinterWorker(StringBuilder out, int maxDisplayRows, int maxDisplayColumnWith) {
        super(out, maxDisplayRows, maxDisplayColumnWith);
    }

    @Override
    StringBuilder print(Index columns, Iterator<DataRow> values) {

        int width = columns.size();
        if (width == 0) {
            return out;
        }

        for (int i = 0; i < maxDisplayRows; i++) {
            if (!values.hasNext()) {
                break;
            }

            if (i > 0) {
                out.append(",");
            }

            DataRow dr = values.next();

            out.append("{");
            for (int j = 0; j < width; j++) {

                if (j > 0) {
                    out.append(",");
                }

                appendTruncate(columns.getColumns()[j]);
                out.append(":");
                appendTruncate(String.valueOf(dr.get(j)));
            }

            out.append("}");
        }

        if (values.hasNext()) {
            out.append(",...");
        }

        return out;
    }

    StringBuilder appendTruncate(String value) {
        return out.append(truncate(value, maxDisplayColumnWith));
    }
}
