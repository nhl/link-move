package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.annotation.AfterSourceRowsConverted;
import com.nhl.link.move.annotation.AfterSourcesMapped;
import com.nhl.link.move.annotation.AfterTargetsCommitted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.annotation.AfterTargetsMatched;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.delete.DeleteSegment;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultCreateOrUpdateBuilderTest {

    private DefaultCreateOrUpdateBuilder<Etl1t> builder;

    @Before
    public void before() {

        ObjAttribute matchAttribute = new ObjAttribute("abc");
        matchAttribute.setType(Object.class.getName());

        ObjEntity targetEntity = new ObjEntity();
        targetEntity.addAttribute(matchAttribute);

        EntityResolver resolver = mock(EntityResolver.class);
        when(resolver.getObjEntity(any(Class.class))).thenReturn(targetEntity);

        ITargetCayenneService cayenneService = mock(ITargetCayenneService.class);
        when(cayenneService.entityResolver()).thenReturn(resolver);

        MapperBuilder mapperBuilder = mock(MapperBuilder.class);

        ITokenManager mockTokenManager = mock(ITokenManager.class);

        this.builder = new DefaultCreateOrUpdateBuilder<>(
                Etl1t.class,
                mock(CreateOrUpdateTargetMerger.class),
                mock(RowConverter.class),
                cayenneService,
                null,
                mockTokenManager,
                mapperBuilder
        );
    }

    @Test
    public void testCreateListenersBuilder() {

        ListenersBuilder listenersBuilder = builder.createListenersBuilder();

        L1 l1 = new L1();
        L2 l2 = new L2();
        L3 l3 = new L3();

        listenersBuilder.addListener(l1);
        listenersBuilder.addListener(l2);
        listenersBuilder.addListener(l3);

        Map<Class<? extends Annotation>, List<StageListener>> listeners = listenersBuilder.getListeners();

        assertNotNull(listeners.get(AfterSourcesMapped.class));
        assertNotNull(listeners.get(AfterSourceRowsConverted.class));
        assertNotNull(listeners.get(AfterTargetsMatched.class));
        assertNotNull(listeners.get(AfterTargetsMapped.class));
        assertNotNull(listeners.get(AfterTargetsMerged.class));
        assertNotNull(listeners.get(AfterTargetsCommitted.class));

        assertEquals(1, listeners.get(AfterSourcesMapped.class).size());
        assertEquals(1, listeners.get(AfterSourceRowsConverted.class).size());
        assertEquals(1, listeners.get(AfterTargetsMatched.class).size());
        assertEquals(2, listeners.get(AfterTargetsMapped.class).size());
        assertEquals(1, listeners.get(AfterTargetsMerged.class).size());
        assertEquals(1, listeners.get(AfterTargetsCommitted.class).size());
    }

    public class L1 {

        @AfterSourcesMapped
        public void m1(CreateOrUpdateSegment<?> s) {

        }

        @AfterSourceRowsConverted
        public void m2(CreateOrUpdateSegment<?> s) {

        }

        @AfterTargetsMatched
        public void m3(CreateOrUpdateSegment<?> s) {

        }

        @AfterTargetsMapped
        public void m4(CreateOrUpdateSegment<?> s) {

        }
    }

    public class L2 {

        @AfterTargetsMapped
        public void m5(CreateOrUpdateSegment<?> s) {

        }

        @AfterTargetsMerged
        public void m6(CreateOrUpdateSegment<?> s) {

        }

        @AfterTargetsCommitted
        public void m7(CreateOrUpdateSegment<?> s) {

        }
    }

    public class L3 {

        public void someMethod(DeleteSegment<?> s) {

        }
    }
}
