package com.nhl.link.move.runtime.task.deleteall;

import com.nhl.link.move.DeleteAllBuilder;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.common.DataSegment;
import com.nhl.link.move.runtime.task.common.TaskStageType;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.map.DbEntity;

import java.lang.annotation.Annotation;

public class DefaultDeleteAllBuilder extends BaseTaskBuilder<DefaultDeleteAllBuilder, DefaultDeleteAllBuilder.NoDataSegment, DefaultDeleteAllBuilder.EmptyStageType> implements DeleteAllBuilder {

    private final ITokenManager tokenManager;
    private final ITargetCayenneService targetCayenneService;
    private final Class<?> type;
    private final DbEntity dbEntity;

    private Expression targetFilter;
    private boolean skipExecutionStats = false;

    public DefaultDeleteAllBuilder(
            Class<?> type,
            ITargetCayenneService targetCayenneService,
            ITokenManager tokenManager,
            DbEntity dbEntity,
            LmLogger logger) {

        super(logger);

        this.tokenManager = tokenManager;
        this.targetCayenneService = targetCayenneService;
        this.type = type;

        this.dbEntity = dbEntity;
    }

    @Override
    protected Class<? extends Annotation>[] supportedListenerAnnotations() {
        return new Class[0];
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
                tokenManager,
                dbEntity,
                skipExecutionStats,
                logger);
    }

    public static final class NoDataSegment implements DataSegment { }

    public enum EmptyStageType implements TaskStageType {
        ;

        @Override
        public Class<? extends Annotation> getLegacyAnnotation() {
            return null;
        }
    }
}
