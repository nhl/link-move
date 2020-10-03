package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.*;
import io.bootique.jdbc.junit5.RowReader;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CreateOrUpdate_ValueConvertersIT extends LmIntegrationTest {

    @Test
    public void test_ById_IntegerToLong() {

        // not specifying "matchById" explicitly ... this should be the default
        LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl6t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl6_to_etl6t_byid.xml").task();

        srcEtl6().insertColumns("id", "name")
                .values(45, "a")
                .values(11, "b")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        assertEquals(2, targetScalar("SELECT count(1) from utest.etl6t"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl6t WHERE NAME = 'a' AND ID = 45"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl6t WHERE NAME = 'b' AND ID = 11"));

        srcEtl6().insertColumns("id", "name").values(31, "c").exec();
        srcEtl6().update().set("name", "d").where("id", 45).exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        assertEquals(3, targetScalar("SELECT count(1) from utest.etl6t"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl6t WHERE NAME = 'd' AND ID = 45"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl6t WHERE NAME = 'c' AND ID = 31"));

        srcEtl6().delete().and("id", 45).exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);
        assertEquals(3, targetScalar("SELECT count(1) from utest.etl6t"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl6t WHERE NAME = 'd' AND ID = 45"));

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void test_ByAttribute_SyncFk() {

        LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl3t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t_converters.xml").matchBy(Etl3t.NAME).task();

        srcEtl2().insertColumns("id", "name").values(34, "abc").exec();
        srcEtl3().insertColumns("e2_id", "name").values(1, "xyz").exec();

        targetRunSql("INSERT INTO utest.etl2t (ID, NAME) VALUES (1, 'abc')");

        Execution e1 = task.run();
        assertExec(1, 1, 0, 0, e1);

        assertEquals(1, targetScalar("SELECT count(1) from utest.etl3t WHERE E2_ID = 1"));
    }

    @Test
    public void test_ById_IntegerToBoolean() {

        LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl4t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl4_to_etl4t_converters.xml")
                .task();

        srcEtl4().insertColumns("id", "c_boolean")
                .values(1, true)
                .values(2, false)
                .exec();

        targetRunSql("INSERT INTO utest.etl4t (ID, C_BOOLEAN) VALUES (1, false)");
        targetRunSql("INSERT INTO utest.etl4t (ID, C_BOOLEAN) VALUES (2, true)");

        Execution e1 = task.run();
        assertExec(2, 0, 2, 0, e1);

        assertEquals(1, targetScalar("SELECT count(1) from utest.etl4t WHERE ID = 1 AND C_BOOLEAN = TRUE"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl4t WHERE ID = 2 AND C_BOOLEAN = FALSE"));
    }

    @Test
    public void test_ById_StringToEnum() {

        LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl4t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl4_to_etl4t_converters.xml")
                .task();

        srcEtl4().insertColumns("id", "c_enum")
                .values(1, "str1")
                .values(2, "str3")
                .exec();

        targetRunSql("INSERT INTO utest.etl4t (ID, C_ENUM) VALUES (1, 'str2')");
        targetRunSql("INSERT INTO utest.etl4t (ID, C_ENUM) VALUES (2, null)");

        Execution e1 = task.run();
        assertExec(2, 0, 2, 0, e1);

        assertEquals(1, targetScalar("SELECT count(1) from utest.etl4t WHERE ID = 1 AND C_ENUM = 'str1'"));
        assertEquals(1, targetScalar("SELECT count(1) from utest.etl4t WHERE ID = 2 AND C_ENUM = 'str3'"));
    }

    @Test
    public void test_ById_DecimalToDecimal() {

        LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl8t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl8_to_etl8t_byid.xml").task();

        BigDecimal c1 = new BigDecimal("1.000000"), c2 = new BigDecimal("1.000000000"), c3 = new BigDecimal("1.000000000");
        createOrUpdateEtl8(1, c1, c2, c3);

        Execution e1 = task.run();
        assertExec(1, 1, 0, 0, e1);

        Execution e2 = task.run();
        assertExec(1, 0, 0, 0, e2);

        c1 = new BigDecimal("1.000001");
        createOrUpdateEtl8(1, c1, c2, c3);

        Execution e3 = task.run();
        assertExec(1, 0, 1, 0, e3);
    }

    @Test
    public void test_ById_DecimalToDecimal_Exception() {

        LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl8t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl8_to_etl8t_byid.xml").task();

        BigDecimal c1 = new BigDecimal("1.000000"), c2 = new BigDecimal("1.000000001"), c3 = new BigDecimal("1.000000000");
        createOrUpdateEtl8(1, c1, c2, c3);

        assertThrows(Exception.class, task::run);
    }

    @Test
    public void test_ImplicitJavaTimeConversion() {

        LmTask task = etl.service(ITaskService.class).createOrUpdate(Etl4t_jt.class)
                .sourceExtractor("com/nhl/link/move/itest/etl4_to_etl4t_jt_implicit.xml")
                .task();

        srcEtl4().insertColumns("id", "c_date", "c_time", "c_timestamp")
                .values(1, "2020-01-02", "08:01:03", "2020-03-04 09:01:04")
                .exec();

        Execution e1 = task.run();
        assertExec(1, 1, 0, 0, e1);

        assertEquals(1, targetScalar("SELECT count(1) from utest.etl4t WHERE ID = 1 AND C_DATE = '2020-01-02' AND C_TIME = '08:01:03' AND C_TIMESTAMP = '2020-03-04 09:01:04'"));
    }

    private void createOrUpdateEtl8(int id, BigDecimal c1, BigDecimal c2, BigDecimal c3) {

        if (getEtl8(id) == null) {
            srcEtl8().insertColumns("id", "c_decimal1", "c_decimal2", "c_decimal3").values(id, c1, c2, c3).exec();
        } else {
            srcEtl8().update()
                    .set("c_decimal1", c1)
                    .set("c_decimal2", c2)
                    .set("c_decimal3", c3)
                    .where("id", id)
                    .exec();
        }

        Object[] etl8 = getEtl8(id);
        assertNotNull(etl8);
        assertEquals(c1, etl8[1]);
        assertEquals(c2, etl8[2]);
        assertEquals(c3, etl8[3]);
    }

    private Object[] getEtl8(int id) {
        List<Object[]> result = srcDb.getTable("etl8").selectStatement(RowReader.arrayReader(4))
                .append("SELECT \"id\", \"c_decimal1\", \"c_decimal2\", \"c_decimal3\" FROM \"etl8\" WHERE ID = ")
                .appendBinding("id", Types.INTEGER, id)
                .select();
        return result.isEmpty() ? null : result.get(0);
    }
}
