package com.nhl.link.move.json.unit;

import com.nhl.link.move.json.unit.cayenne.t.Etlt1;
import io.bootique.BQRuntime;
import io.bootique.Bootique;
import io.bootique.cayenne.v42.CayenneModule;
import io.bootique.cayenne.v42.junit5.CayenneTester;
import io.bootique.jdbc.junit5.Table;
import io.bootique.jdbc.junit5.derby.DerbyTester;
import io.bootique.junit5.BQApp;
import io.bootique.junit5.BQTest;
import io.bootique.junit5.BQTestScope;
import io.bootique.junit5.BQTestTool;

@BQTest
public abstract class DerbyTargetTest {

    @BQTestTool(BQTestScope.GLOBAL)
    protected static final DerbyTester targetDb = DerbyTester.db()
            .initDB("classpath:com/nhl/link/move/json/target-schema-derby.sql");

    @BQTestTool(BQTestScope.GLOBAL)
    protected static final CayenneTester targetCayenne = CayenneTester.create()
            .deleteBeforeEachTest()
            .skipSchemaCreation()
            .entities(Etlt1.class);

    @BQApp(value = BQTestScope.GLOBAL, skipRun = true)
    protected static final BQRuntime targetApp = Bootique.app()
            .autoLoadModules()
            .module(targetDb.moduleWithTestDataSource("target_db"))
            .module(targetCayenne.moduleWithTestHooks())
            .module(b -> CayenneModule.extend(b).addLocation("classpath:com/nhl/link/move/json/cayenne-targets.xml"))
            .createRuntime();

    protected Table etlt1() {
        return targetDb.getTable("etlt1");
    }
}
