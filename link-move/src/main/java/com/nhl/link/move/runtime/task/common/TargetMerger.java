package com.nhl.link.move.runtime.task.common;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.link.move.runtime.targetmodel.TargetAttribute;
import com.nhl.link.move.writer.TargetPropertyWriterFactory;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;

import java.util.Map;
import java.util.Set;

/**
 * @since 2.12
 */
public abstract class TargetMerger<T extends DataObject> {

    private static final String LM_COLUMN_PREFIX = "$lm_";

    protected TargetPropertyWriterFactory<T> writerFactory;
    protected FkResolver fkResolver;

    public TargetMerger(TargetPropertyWriterFactory<T> writerFactory, FkResolver fkResolver) {
        this.writerFactory = writerFactory;
        this.fkResolver = fkResolver;
    }

    public DataFrame merge(ObjectContext context, DataFrame df) {
        return merge(resolveFks(context, df));
    }

    // TODO: create a standalone stage for FK resolving?
    protected DataFrame resolveFks(ObjectContext context, DataFrame df) {
        Map<TargetAttribute, Set<Object>> fks = fkResolver.collectFks(df, dataColumns(df));
        Map<TargetAttribute, Map<Object, Object>> related = fkResolver.fetchRelated(context, fks);

        return fkResolver.resolveFks(df, related);
    }

    protected abstract DataFrame merge(DataFrame df);

    protected Index dataColumns(DataFrame df) {
        return df.getColumnsIndex().dropLabels(s -> s.startsWith(LM_COLUMN_PREFIX));
    }
}
