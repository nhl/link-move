package com.nhl.link.move.json.unit;

import com.nhl.link.move.json.unit.cayenne.t.Etlt1;
import io.bootique.BQRuntime;
import io.bootique.Bootique;
import io.bootique.cayenne.v50.CayenneModule;
import io.bootique.cayenne.v50.junit.CayenneTester;
import io.bootique.jdbc.junit.Table;
import io.bootique.jdbc.junit.derby.DerbyTester;
import io.bootique.junit.BQApp;
import io.bootique.junit.BQTest;
import io.bootique.junit.BQTestScope;
import io.bootique.junit.BQTestTool;

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
