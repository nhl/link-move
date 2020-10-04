package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ExtractorReloadingIT extends LmIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractorReloadingIT.class);
    private static final String EXTRACTOR = "etl1_to_etl1t.xml";

    @TempDir
    static File extractorsDir;

    static void initExtractor(String sourceFromClasspath) {

        try (InputStream in = ExtractorReloadingIT.class.getClassLoader().getResourceAsStream(sourceFromClasspath)) {

            assertNotNull(in, "Invalid resource: " + sourceFromClasspath);

            File extractor = new File(extractorsDir, EXTRACTOR);
            Files.copy(in, extractor.toPath(), StandardCopyOption.REPLACE_EXISTING);
            LOGGER.info("Updated extractor from {}. File timestamp: {}", sourceFromClasspath, extractor.lastModified());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected LmRuntimeBuilder testRuntimeBuilder() {
        return super.testRuntimeBuilder().extractorModelsRoot(extractorsDir);
    }

    @Test
    public void testReload() throws InterruptedException {

        initExtractor("com/nhl/link/move/itest/etl1_to_etl1t_v1.xml");

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor(EXTRACTOR)
                .matchBy("db:name")
                .task();

        srcEtl1().insertColumns("name", "age", "description")
                .values("a", 3, "d1")
                .values("b", null, "d2")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", 3).eq("description", null).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", null).eq("description", null).assertOneMatch();

        // sleep to make sure extractor file name timestamp change is detectable
        Thread.sleep(1000);
        initExtractor("com/nhl/link/move/itest/etl1_to_etl1t_v2.xml");

        Execution e2 = task.run();
        assertExec(2, 0, 2, 0, e2);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", 3).eq("description", "d1").assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", null).eq("description", "d2").assertOneMatch();
    }
}
