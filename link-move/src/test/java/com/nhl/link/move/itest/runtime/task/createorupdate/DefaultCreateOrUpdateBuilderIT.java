package com.nhl.link.move.itest.runtime.task.createorupdate;

import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.TargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateTargetMerger;
import com.nhl.link.move.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.unit.DerbySrcTargetTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class DefaultCreateOrUpdateBuilderIT extends DerbySrcTargetTest {

    private DefaultCreateOrUpdateBuilder builder;

    @BeforeEach
    public void createBuilder() {
        this.builder = new DefaultCreateOrUpdateBuilder(
                Etl1t.class,
                mock(CreateOrUpdateTargetMerger.class),
                mock(FkResolver.class),
                mock(RowConverter.class),
                new TargetCayenneService(targetCayenne.getRuntime()),
                mock(IExtractorService.class),
                mock(MapperBuilder.class),
                mock(LmLogger.class));
    }

    @Test
    public void testTask_NoExtractorName() {
        assertThrows(IllegalStateException.class, builder::task);
    }

    @Test
    public void testTask_ExtractorPresent() {
        builder.sourceExtractor("test.xml");
        assertNotNull(builder.task());
    }
}
