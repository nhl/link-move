package com.nhl.link.move.runtime.json.it;

import com.nhl.link.move.Execution;
import com.nhl.link.move.model.cayenne.TestTime;
import com.nhl.link.move.runtime.LmRuntime;
import io.bootique.BQRuntime;
import io.bootique.jdbc.tomcat.TomcatJdbcModule;
import io.bootique.linkmove.LinkMoveModuleProvider;
import io.bootique.test.junit.BQTestFactory;
import org.junit.Rule;
import org.junit.Test;

public class CreateOrUpdate_fromJsonIT extends LmItTest {

    @Rule
    public BQTestFactory testFactory = new BQTestFactory();

    @Test
    public void readJsonDates_test() {
        BQRuntime bqRuntime = testFactory
                .app("--config=classpath:json_it.yml")
                .module(new LinkMoveModuleProvider())
                .module(TomcatJdbcModule::new)
                .createRuntime();
        LmRuntime lmRuntime = bqRuntime.getInstance(LmRuntime.class);
        Execution execution = lmRuntime.getTaskService().createOrUpdate(TestTime.class)
                .sourceExtractor("/src/test/resources/extractor/it/time_sync.xml")
                .task()
                .run();
        assertExec(3, 3, 0, 0, execution);
    }
}
