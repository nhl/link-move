package com.nhl.link.move.runtime.task.deleteall;

import com.nhl.link.move.DeleteAllBuilder;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.common.DataSegment;
import com.nhl.link.move.runtime.task.common.TaskStageType;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.map.DbEntity;

import java.lang.annotation.Annotation;

/**
 * @since 3.0.0
 */
public class DefaultDeleteAllBuilder extends BaseTaskBuilder<DefaultDeleteAllBuilder, DefaultDeleteAllBuilder.NoDataSegment, DefaultDeleteAllBuilder.EmptyStageType> implements DeleteAllBuilder {

    private final ITargetCayenneService targetCayenneService;
    private final Class<?> type;
    private final DbEntity dbEntity;

    private Expression targetFilter;
    private boolean skipExecutionStats = false;

    public DefaultDeleteAllBuilder(
            Class<?> type,
            ITargetCayenneService targetCayenneService,
            DbEntity dbEntity,
            LmLogger logger) {

        super(logger);

        this.targetCayenneService = targetCayenneService;
        this.type = type;

        this.dbEntity = dbEntity;
    }

    @Override
    public DefaultDeleteAllBuilder targetFilter(Expression filter) {
        this.targetFilter = filter;
        return this;
    }

    @Override
    public DefaultDeleteAllBuilder skipExecutionStats() {
        this.skipExecutionStats = true;
        return this;
    }

    @Override
    public DeleteAllTask task() throws IllegalStateException {
        return new DeleteAllTask(type,
                targetFilter,
                targetCayenneService,
                dbEntity,
                skipExecutionStats,
                logger);
    }

    public static final class NoDataSegment implements DataSegment { }

    public enum EmptyStageType implements TaskStageType {
    }
}
