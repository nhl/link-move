package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.t.Etl11t;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.unit.cayenne.t.Etl3t;
import com.nhl.link.move.unit.cayenne.t.Etl5t;
import com.nhl.link.move.unit.cayenne.t.Etl9t;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CreateOrUpdateIT extends LmIntegrationTest {

    @Test
    public void test_MatchByAttribute() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
                .matchBy(Etl1t.NAME)
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", null).assertOneMatch();

        srcEtl1().insertColumns("name").values("c").exec();
        srcEtl1().update().set("age", 5).where("name", "a").exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        etl1t().matcher().assertMatches(3);
        etl1t().matcher().eq("name", "a").eq("age", 5).assertOneMatch();
        etl1t().matcher().eq("name", "c").eq("age", null).assertOneMatch();

        srcEtl1().delete().where("name", "a").exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);
        etl1t().matcher().assertMatches(3);
        etl1t().matcher().eq("name", "a").eq("age", 5).assertOneMatch();

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void test_MatchByDbAttribute() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
                .matchBy("db:name")
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", null).assertOneMatch();

        srcEtl1().insertColumns("name").values("c").exec();
        srcEtl1().update().set("age", 5).where("name", "a").exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        etl1t().matcher().assertMatches(3);
        etl1t().matcher().eq("name", "a").eq("age", 5).assertOneMatch();
        etl1t().matcher().eq("name", "c").eq("age", null).assertOneMatch();

        srcEtl1().delete().where("name", "a").exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);
        etl1t().matcher().assertMatches(3);
        etl1t().matcher().eq("name", "a").eq("age", 5).assertOneMatch();

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void test_MatchByDbAttribute_Binary() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl11t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl11_to_etl11t.xml")
                .matchBy("db:bin")
                .task();

        srcEtl11().insertColumns("id", "bin")
                .values(1, new byte[]{4, 5, 6})
                .values(2, new byte[]{1, 3, 8})
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl11t().matcher().assertMatches(2);

        srcEtl11().insertColumns("id", "bin")
                .values(3, new byte[]{7, 11, 2})
                .exec();

        srcEtl11().update().set("bin", new byte[]{13, 14, 15}).where("id", 2).exec();

        Execution e2 = task.run();
        assertExec(3, 2, 0, 0, e2);
        etl11t().matcher().assertMatches(4);
    }

    @Test
    public void test_MatchByAttributes() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
                .matchBy(Etl1t.NAME, Etl1t.AGE)
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", 5)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", 5).assertOneMatch();

        // changing one of the key components should result in no-match and a new record insertion
        srcEtl1().update().set("name", "c").where("name", "a").exec();

        Execution e2 = task.run();
        assertExec(2, 1, 0, 0, e2);
        etl1t().matcher().assertMatches(3);
        etl1t().matcher().eq("name", "c").eq("age", 3).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", 5).assertOneMatch();
        etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void test_MatchById() {

        LmTask task = lmRuntime.service(ITaskService.class).
                createOrUpdate(Etl5t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml")
                .matchById()
                .task();

        srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);

        etl5t().matcher().assertMatches(2);
        etl5t().matcher().eq("name", "a").eq("id", 45).assertOneMatch();
        etl5t().matcher().eq("name", "b").eq("id", 11).assertOneMatch();

        srcEtl5().insertColumns("id", "name").values(31, "c").exec();
        srcEtl5().update().set("name", "d").where("id", 45).exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        etl5t().matcher().assertMatches(3);
        etl5t().matcher().eq("name", "d").eq("id", 45).assertOneMatch();
        etl5t().matcher().eq("name", "c").eq("id", 31).assertOneMatch();

        srcEtl5().delete().where("id", 45).exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);

        etl5t().matcher().assertMatches(3);
        etl5t().matcher().eq("name", "d").eq("id", 45).assertOneMatch();

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void test_MatchById_Default() {

        // not specifying "matchById" explicitly ... this should be the default
        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl5t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl5_to_etl5t_byid.xml")
                .task();

        srcEtl5().insertColumns("id", "name").values(45, "a").values(11, "b").exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl5t().matcher().assertMatches(2);
        etl5t().matcher().eq("name", "a").eq("id", 45).assertOneMatch();
        etl5t().matcher().eq("name", "b").eq("id", 11).assertOneMatch();

        srcEtl5().insertColumns("id", "name").values(31, "c").exec();
        srcEtl5().update().set("name", "d").where("id", 45).exec();

        Execution e2 = task.run();
        assertExec(3, 1, 1, 0, e2);
        etl5t().matcher().assertMatches(3);
        etl5t().matcher().eq("name", "d").eq("id", 45).assertOneMatch();
        etl5t().matcher().eq("name", "c").eq("id", 31).assertOneMatch();

        srcEtl5().delete().where("id", 45).exec();

        Execution e3 = task.run();
        assertExec(2, 0, 0, 0, e3);
        etl5t().matcher().assertMatches(3);
        etl5t().matcher().eq("name", "d").eq("id", 45).assertOneMatch();

        Execution e4 = task.run();
        assertExec(2, 0, 0, 0, e4);
    }

    @Test
    public void test_MatchById_Autoincrement() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_byid.xml")
                .matchById()
                .task();

        srcEtl1().insertColumns("id", "name", "age")
                .values(45, "a", 67)
                .values(11, "b", 4)
                .exec();

        assertThrows(LmRuntimeException.class, task::run);
    }

    @Test
    public void test_CapsLower() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_lower.xml")
                .matchBy(Etl1t.NAME)
                .task();

        srcEtl1().insertColumns("name", "age")
                .values("a", 3)
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", 3).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", null).assertOneMatch();
    }

    @Test
    public void test_ExtraSrcColumns() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_extra_source_columns.xml")
                .matchBy(Etl1t.NAME)
                .task();

        srcEtl1().insertColumns("name", "description")
                .values("a", "dd")
                .values("b", null)
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl1t().matcher().assertMatches(2);
        etl1t().matcher().eq("name", "a").eq("age", null).eq("description", null).assertOneMatch();
        etl1t().matcher().eq("name", "b").eq("age", null).eq("description", null).assertOneMatch();
    }

    @Test
    public void test_MatchByAttribute_SyncFk() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl3t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t.xml")
                .matchBy(Etl3t.NAME)
                .task();

        srcEtl2().insertColumns("id", "address", "name")
                .values(34, "Address1", "2Name1")
                .values(58, "Address2", "2Name2")
                .exec();
        srcEtl5().insertColumns("id", "name").values(17, "5Name1").values(11, "5Name2").exec();
        srcEtl3().insertColumns("e2_id", "e5_id", "name", "phone_number")
                .values(58, 17, "3Name1", "3PHONE1")
                .values(34, 17, "3Name2", "3PHONE2")
                .exec();

        etl2t().insertColumns("id", "address", "name")
                .values(34, "Address1", "2Name1")
                .values(58, "Address2", "2Name2")
                .exec();
        etl5t().insertColumns("id", "name")
                .values(17, "5Name1")
                .values(11, "5Name2")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl3t().matcher().assertMatches(2);
        etl3t().matcher().eq("name", "3Name1").eq("phone_number", "3PHONE1").eq("e2_id", 58).eq("e5_id", 17).assertOneMatch();
        etl3t().matcher().eq("name", "3Name2").eq("phone_number", "3PHONE2").eq("e2_id", 34).eq("e5_id", 17).assertOneMatch();
    }

    @Test
    public void test_MatchByAttribute_SyncFk_Nulls() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl3t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl3_to_etl3t.xml")
                .matchBy(Etl3t.NAME)
                .task();

        srcEtl2().insertColumns("id", "address", "name").values(34, "Address1", "2Name1").exec();
        srcEtl5().insertColumns("id", "name").values(17, "5Name1").values(11, "5Name2").exec();
        srcEtl3().insertColumns("e2_id", "e5_id", "name", "phone_number")
                .values(null, 17, "3Name1", "3PHONE1")
                .values(34, null, "3Name2", "3PHONE2")
                .exec();

        etl2t().insertColumns("id", "address", "name").values(34, "Address1", "2Name1").exec();
        etl5t().insertColumns("id", "name")
                .values(17, "5Name1")
                .values(11, "5Name2")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
        etl3t().matcher().assertMatches(2);
        etl3t().matcher().eq("name", "3Name1").eq("phone_number", "3PHONE1").eq("e2_id", null).eq("e5_id", 17).assertOneMatch();
        etl3t().matcher().eq("name", "3Name2").eq("phone_number", "3PHONE2").eq("e2_id", 34).eq("e5_id", null).assertOneMatch();

        srcEtl3().update().set("e5_id", null).where("name", "3Name1").exec();
        srcEtl3().update().set("e5_id", 11).where("name", "3Name2").exec();

        Execution e2 = task.run();
        assertExec(2, 0, 2, 0, e2);
        etl3t().matcher().assertMatches(2);
        etl3t().matcher().eq("name", "3Name1").eq("phone_number", "3PHONE1").eq("e2_id", null).eq("e5_id", null).assertOneMatch();
        etl3t().matcher().eq("name", "3Name2").eq("phone_number", "3PHONE2").eq("e2_id", 34).eq("e5_id", 11).assertOneMatch();
    }

    @Test
    public void test_MatchByAttribute_SyncNulls() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl1t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl1_to_etl1t_upper.xml")
                .matchBy(Etl1t.NAME)
                .task();

        etl1t().insertColumns("name", "age").values("a", 3).exec();
        srcEtl1().insertColumns("name", "age").values("a", null).exec();

        Execution e1 = task.run();
        assertExec(1, 0, 1, 0, e1);
        etl1t().matcher().assertOneMatch();
        etl1t().matcher().eq("name", "a").eq("age", null).assertOneMatch();
    }

    @Test
    public void test_MatchById_RelationshipOnPK() {

        LmTask task = lmRuntime.service(ITaskService.class)
                .createOrUpdate(Etl9t.class)
                .sourceExtractor("com/nhl/link/move/itest/etl9_to_etl9t.xml")
                .matchById()
                .task();

        srcEtl9().insertColumns("id", "name")
                .values(1, "a")
                .values(2, "b")
                .exec();

        Execution e1 = task.run();
        assertExec(2, 2, 0, 0, e1);
    }
}
