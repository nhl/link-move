package com.nhl.link.move.unit;

import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.query.SQLSelect;
import org.apache.cayenne.query.SQLTemplate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public abstract class DerbySrcTargetTest extends DerbySrcTest {

    protected static CayenneDerbyStack targetStack;
    protected ObjectContext targetContext;

    @BeforeAll
    public static void startTarget() {
        targetStack = new CayenneDerbyStack("derbytarget", "cayenne-linketl-tests-targets.xml");
    }

    @AfterAll
    public static void shutdownTarget() {
        targetStack.shutdown();
    }

    @BeforeEach
    public void prepareTarget() {

        targetContext = targetStack.newContext();

        // first query in a test set will also load the schema...

        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from utest.etl1t"));
        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from utest.etl3t"));
        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from utest.etl2t"));
        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from utest.etl4t"));
        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from utest.etl5t"));
        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from utest.etl11t"));

        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from \"utest\".\"ti_sub1\""));
        targetContext.performGenericQuery(new SQLTemplate(Etl1t.class, "DELETE from \"utest\".\"ti_super\""));
    }

    protected void targetRunSql(String sql) {
        targetContext.performGenericQuery(new SQLTemplate(Object.class, sql));
    }

    protected int targetScalar(String sql) {
        SQLSelect<Integer> query = SQLSelect.scalarQuery(Integer.class, sql);
        return query.selectOne(targetContext);
    }

}
