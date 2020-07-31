package com.nhl.link.move.itest.runtime.task.createorupdate;

import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.cayenne.TargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateTargetMerger;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.DerbySrcTargetTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class DefaultCreateOrUpdateBuilderIT extends DerbySrcTargetTest {

    private ITargetCayenneService cayenneService;

    @Before
    public void before() {
        this.cayenneService = new TargetCayenneService(targetStack.runtime());
    }

    @Test(expected = IllegalStateException.class)
    public void testTask_NoExtractorName() {


        DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(
                Etl1t.class,
                mock(CreateOrUpdateTargetMerger.class),
                mock(RowConverter.class),
                cayenneService,
                mock(IExtractorService.class),
                mock(ITokenManager.class),
                mock(MapperBuilder.class));

        builder.task();
    }

    @Test
    public void testTask_ExtractorPresent() {
        DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(
                Etl1t.class,
                mock(CreateOrUpdateTargetMerger.class),
                mock(RowConverter.class),
                cayenneService,
                mock(IExtractorService.class),
                mock(ITokenManager.class),
                mock(MapperBuilder.class));

        builder.sourceExtractor("test.xml");
        assertNotNull(builder.task());
    }
}
