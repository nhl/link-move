package com.nhl.link.move.runtime;

import com.nhl.link.move.resource.ResourceResolver;
import com.nhl.link.move.runtime.task.ITaskService;
import org.apache.cayenne.CayenneDataObject;
import org.apache.cayenne.DataChannel;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.di.Binder;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.reflect.ClassDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class LmRuntimeBuilderTest {

    private ServerRuntime cayenneRuntime;

    @BeforeEach
    public void before() {

        DbEntity mockDbEntity = mock(DbEntity.class);
        when(mockDbEntity.getName()).thenReturn("medb");
        when(mockDbEntity.getAttributes()).thenReturn(Collections.emptyList());

        ObjEntity mockEntity = mock(ObjEntity.class);
        when(mockEntity.getName()).thenReturn("me");
        when(mockEntity.getDbEntity()).thenReturn(mockDbEntity);

        ClassDescriptor mockDescriptor = mock(ClassDescriptor.class);

        EntityResolver resolver = mock(EntityResolver.class);
        when(resolver.getObjEntity(any(Class.class))).thenReturn(mockEntity);
        when(resolver.getClassDescriptor(any(String.class))).thenReturn(mockDescriptor);

        DataChannel channel = mock(DataChannel.class);
        when(channel.getEntityResolver()).thenReturn(resolver);

        this.cayenneRuntime = mock(ServerRuntime.class);
        when(cayenneRuntime.getChannel()).thenReturn(channel);
    }

    @Test
    public void build_DefaultConfigLoader() {

        LmRuntimeBuilder builder = LmRuntime.builder().targetRuntime(cayenneRuntime);
        LmRuntime runtime = builder.build();

        assertNotNull(runtime);
        ITaskService taskService = runtime.service(ITaskService.class);
        assertNotNull(taskService);
        assertNotNull(taskService.createOrUpdate(CayenneDataObject.class));
    }

    @Test
    public void build_NoTargetRuntime() {
        LmRuntimeBuilder builder = LmRuntime.builder()
                .extractorResolver(mock(ResourceResolver.class));
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void build() {
        LmRuntimeBuilder builder = LmRuntime.builder()
                .extractorResolver(mock(ResourceResolver.class))
                .targetRuntime(cayenneRuntime);
        LmRuntime runtime = builder.build();
        assertNotNull(runtime);

        ITaskService taskService = runtime.service(ITaskService.class);
        assertNotNull(taskService);
        assertNotNull(taskService.createOrUpdate(CayenneDataObject.class));
    }

    @Test
    public void adapter() {

        LmAdapter a1 = mock(LmAdapter.class);
        LmAdapter a2 = mock(LmAdapter.class);

        LmRuntimeBuilder builder = LmRuntime.builder()
                .targetRuntime(cayenneRuntime)
                .adapter(a1)
                .adapter(a2);

        verifyZeroInteractions(a1);
        verifyZeroInteractions(a2);
        builder.build();
        verify(a1).configure(any(Binder.class));
        verify(a2).configure(any(Binder.class));
    }

}
