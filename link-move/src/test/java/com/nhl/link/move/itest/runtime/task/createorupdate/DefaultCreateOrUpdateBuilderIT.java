package com.nhl.link.move.itest.runtime.task.createorupdate;

import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.cayenne.TargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateTargetMerger;
import com.nhl.link.move.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.DerbySrcTargetTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class DefaultCreateOrUpdateBuilderIT extends DerbySrcTargetTest {

    private ITargetCayenneService cayenneService;

    @BeforeEach
    public void before() {
        this.cayenneService = new TargetCayenneService(cayenne.getRuntime());
    }

    @Test
    public void testTask_NoExtractorName() {

        DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(
                Etl1t.class,
                mock(CreateOrUpdateTargetMerger.class),
                mock(FkResolver.class),
                mock(RowConverter.class),
                cayenneService,
                mock(IExtractorService.class),
                mock(ITokenManager.class),
                mock(MapperBuilder.class));

        assertThrows(IllegalStateException.class, builder::task);
    }

    @Test
    public void testTask_ExtractorPresent() {
        DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(
                Etl1t.class,
                mock(CreateOrUpdateTargetMerger.class),
                mock(FkResolver.class),
                mock(RowConverter.class),
                cayenneService,
                mock(IExtractorService.class),
                mock(ITokenManager.class),
                mock(MapperBuilder.class));

        builder.sourceExtractor("test.xml");
        assertNotNull(builder.task());
    }
}
