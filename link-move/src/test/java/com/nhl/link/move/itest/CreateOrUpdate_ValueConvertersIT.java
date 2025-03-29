package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateOrUpdate_ValueConvertersIT extends LmIntegrationTest {

    @Test
    public void byId_IntegerToLong() {

        // not specifying "matchById" explicitly ... this should be the default
        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl6t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml")
                .task();

        srcEtl6().insertColumns("id", "name")
                .values(45, "a")
                .values(11, "b")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl6t().matcher().assertMatches(2);
        etl6t().matcher().eq("name", "a").andEq("id", 45).assertOneMatch();
        etl6t().matcher().eq("name", "b").andEq("id", 11).assertOneMatch();

        srcEtl6().insertColumns("id", "name").values(31, "c").exec();
        srcEtl6().update().set("name", "d").where("id", 45).exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        etl6t().matcher().assertMatches(3);
        etl6t().matcher().eq("name", "d").andEq("id", 45).assertOneMatch();
        etl6t().matcher().eq("name", "c").andEq("id", 31).assertOneMatch();

        srcEtl6().delete().where("id", 45).exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);
        etl6t().matcher().assertMatches(3);
        etl6t().matcher().eq("name", "d").andEq("id", 45).assertOneMatch();

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void byAttribute_SyncFk() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl3t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t_converters.xml")
                .matchBy(Etl3t.NAME)
                .task();

        srcEtl2().insertColumns("id", "name").values(1, "abc").exec();
        srcEtl3().insertColumns("e2_id", "name").values(1, "xyz").exec();

        etl2t().insertColumns("id", "name").values(1, "abc").exec();

        Execution e1 = task.run();
        assertExec(1, 1, 0, 0, e1);

        etl3t().matcher().eq("e2_id", 1).assertOneMatch();
    }

    @Test
    public void byId_IntegerToBoolean() {

        LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl4t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl4_to_etl4t_converters.xml")
                .task();

        srcEtl4().insertColumns("id", "c_boolean")
                .values(1, true)
                .values(2, false)
                .exec();

        etl4t().insertColumns("id", "c_boolean")
                .values(1, false)
                .values(2, true)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 0, 2, 0, e1);

        etl4t().matcher().eq("id", 1).andEq("c_boolean", true).assertOneMatch();
        etl4t().matcher().eq("id", 2).andEq("c_boolean", false).assertOneMatch();
    }

    @Test
    public void byId_StringToEnum() {

        LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl4t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl4_to_etl4t_converters.xml")
                .task();

        srcEtl4().insertColumns("id", "c_enum")
                .values(1, "str1")
                .values(2, "str3")
                .exec();

        etl4t().insertColumns("id", "c_enum")
                .values(1, "str2")
                .values(2, null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 0, 2, 0, e1);

        etl4t().matcher().eq("id", 1).andEq("c_enum", "str1").assertOneMatch();
        etl4t().matcher().eq("id", 2).andEq("c_enum", "str3").assertOneMatch();
    }

    @Test
    public void byId_DecimalToDecimal() {

        LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl8t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl8_to_etl8t_byid.xml").task();

        srcEtl8().insertColumns("id", "c_decimal1", "c_decimal2", "c_decimal3")
                .values(1, new BigDecimal("1.000000"), new BigDecimal("1.000000000"), new BigDecimal("1.000000000"))
                .exec();

        Execution e1 = task.run();
        assertExec(1, 1, 0, 0, e1);

        Execution e2 = task.run();
        assertExec(1, 0, 0, 0, e2);

        srcEtl8().update()
                .set("c_decimal1", new BigDecimal("1.000001"))
                .where("id", 1)
                .exec();

        Execution e3 = task.run();
        assertExec(1, 0, 1, 0, e3);
    }

    @Test
    @Disabled("Until https://github.com/bootique/bootique-jdbc/issues/104 is fixed")
    public void byId_DecimalToDecimal_Exception() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl8t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl8_to_etl8t_byid.xml")
                .task();

        srcEtl8().insertColumns("id", "c_decimal2")
                .values(1, new BigDecimal("1.000000001"))
                .exec();

        assertThrows(Exception.class, task::run, "Expected exception due to precision loss for 'c_decimal2'");
    }

    @Test
    public void implicitJavaTimeConversion() {

        LmTask task = lmRuntime.service(ITaskService.class).createOrUpdate(Etl4t_jt.class)
                .sourceExtractor("com/nhl/link/move/itest/etl4_to_etl4t_jt_implicit.xml")
                .task();

        srcEtl4().insertColumns("id", "c_date", "c_time", "c_timestamp")
                .values(1, "2020-01-02", "08:01:03", "2020-03-04 09:01:04")
                .exec();

        Execution e1 = task.run();
        assertExec(1, 1, 0, 0, e1);

        etl4t().matcher()
                .eq("id", 1).eq("c_date", "2020-01-02").andEq("c_time", "08:01:03").andEq("c_timestamp", "2020-03-04 09:01:04")
                .assertOneMatch();
    }
}
