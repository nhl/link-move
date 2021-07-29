package com.nhl.link.move.unit;

import com.nhl.link.move.unit.cayenne.t.*;
import io.bootique.BQRuntime;
import io.bootique.Bootique;
import io.bootique.cayenne.CayenneModule;
import io.bootique.cayenne.junit5.CayenneTester;
import io.bootique.jdbc.junit5.Table;
import io.bootique.jdbc.junit5.derby.DerbyTester;
import io.bootique.junit5.BQApp;
import io.bootique.junit5.BQTestScope;
import io.bootique.junit5.BQTestTool;

public abstract class DerbySrcTargetTest extends DerbySrcTest {

    @BQTestTool(BQTestScope.GLOBAL)
    protected static final DerbyTester targetDb = DerbyTester.db()
            .initDB("classpath:com/nhl/link/move/itest/target-schema-derby.sql");

    @BQTestTool(BQTestScope.GLOBAL)
    protected static final CayenneTester targetCayenne = CayenneTester.create()
            .deleteBeforeEachTest()
            .skipSchemaCreation()
            //  TODO: workaround for a CayenneTester bug: generator skips the "sub" table in vertical
            //   inheritance schema.
            .tables("ti_super", "ti_sub1")
            .entities(Etl1t.class,
                    Etl3t.class,
                    Etl2t.class,
                    Etl4t.class,
                    Etl4t_jt.class,
                    Etl5t.class,
                    Etl6t.class,
                    Etl7t.class,
                    Etl8t.class,
                    Etl9t.class,
                    Etl10t.class,
                    Etl11t.class);

    @BQApp(value = BQTestScope.GLOBAL, skipRun = true)
    protected static final BQRuntime targetApp = Bootique.app()
            .autoLoadModules()
            .module(targetDb.moduleWithTestDataSource("target_db"))
            .module(targetCayenne.moduleWithTestHooks())
            .module(b -> CayenneModule.extend(b).addProject("cayenne-linketl-tests-targets.xml"))
            .createRuntime();

    protected Table etl1t() {
        return targetDb.getTable("etl1t");
    }

    protected Table etl2t() {
        return targetDb.getTable("etl2t");
    }

    protected Table etl3t() {
        return targetDb.getTable("etl3t");
    }

    protected Table etl4t() {
        return targetDb.getTable("etl4t");
    }

    protected Table etl5t() {
        return targetDb.getTable("etl5t");
    }

    protected Table etl6t() {
        return targetDb.getTable("etl6t");
    }

    protected Table etl7t() {
        return targetDb.getTable("etl7t");
    }

    protected Table etl11t() {
        return targetDb.getTable("etl11t");
    }

    protected Table tiSuper() {
        return targetDb.getTable("ti_super");
    }

    protected Table tiSub1() {
        return targetDb.getTable("ti_sub1");
    }
}
