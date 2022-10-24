package com.nhl.link.move.unit;

import io.bootique.BQRuntime;
import io.bootique.Bootique;
import io.bootique.jdbc.junit5.Table;
import io.bootique.jdbc.junit5.derby.DerbyTester;
import io.bootique.junit5.BQApp;
import io.bootique.junit5.BQTest;
import io.bootique.junit5.BQTestScope;
import io.bootique.junit5.BQTestTool;

@BQTest
public abstract class DerbySrcTest {

    @BQTestTool(BQTestScope.GLOBAL)
    protected static final DerbyTester srcDb = DerbyTester.db()
            .initDB("classpath:com/nhl/link/move/itest/src-schema-derby.sql")
            .deleteBeforeEachTest("etl1", "etl2", "etl4", "etl5", "etl3", "etl6", "etl7", "etl8", "etl9", "etl11", "etl_sub1");

    @BQApp(value = BQTestScope.GLOBAL, skipRun = true)
    protected static final BQRuntime srcApp = Bootique.app()
            .autoLoadModules()
            .module(srcDb.moduleWithTestDataSource("src_db"))
            .createRuntime();

    protected Table srcEtl1() {
        return srcDb.getTable("etl1");
    }

    protected Table srcEtl2() {
        return srcDb.getTable("etl2");
    }

    protected Table srcEtl3() {
        return srcDb.getTable("etl3");
    }

    protected Table srcEtl4() {
        return srcDb.getTable("etl4");
    }

    protected Table srcEtl5() {
        return srcDb.getTable("etl5");
    }

    protected Table srcEtl6() {
        return srcDb.getTable("etl6");
    }

    protected Table srcEtl7() {
        return srcDb.getTable("etl7");
    }

    protected Table srcEtl8() {
        return srcDb.getTable("etl8");
    }

    protected Table srcEtl9() {
        return srcDb.getTable("etl9");
    }

    protected Table srcEtl11() {
        return srcDb.getTable("etl11");
    }

    protected Table srcEtlSub1() {
        return srcDb.getTable("etl_sub1");
    }

    protected Table srcEtl12() {
        return srcDb.getTable("etl12");
    }
}
