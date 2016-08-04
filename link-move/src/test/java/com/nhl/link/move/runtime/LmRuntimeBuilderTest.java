package com.nhl.link.move.runtime;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.cayenne.CayenneDataObject;
import org.apache.cayenne.DataChannel;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.di.Binder;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.runtime.LmRuntime;
import com.nhl.link.move.runtime.LmRuntimeBuilder;
import com.nhl.link.move.runtime.adapter.LinkEtlAdapter;
import com.nhl.link.move.runtime.extractor.model.IExtractorModelLoader;
import com.nhl.link.move.runtime.task.ITaskService;

public class LmRuntimeBuilderTest {

	private ServerRuntime cayenneRuntime;

	@Before
	public void before() {

		ObjEntity mockEntity = mock(ObjEntity.class);
		when(mockEntity.getName()).thenReturn("me");

		EntityResolver resolver = mock(EntityResolver.class);
		when(resolver.getObjEntity(any(Class.class))).thenReturn(mockEntity);

		DataChannel channel = mock(DataChannel.class);
		when(channel.getEntityResolver()).thenReturn(resolver);

		this.cayenneRuntime = mock(ServerRuntime.class);
		when(cayenneRuntime.getChannel()).thenReturn(channel);
	}

	@Test
	public void testBuild_DefaultConfigLoader() {

		LmRuntimeBuilder builder = new LmRuntimeBuilder().withTargetRuntime(cayenneRuntime);
		LmRuntime runtime = builder.build();

		assertNotNull(runtime);
		ITaskService taskService = runtime.service(ITaskService.class);
		assertNotNull(taskService);
		assertNotNull(taskService.createOrUpdate(CayenneDataObject.class));
	}

	@Test(expected = IllegalStateException.class)
	public void testBuild_NoTargetRuntime() {
		LmRuntimeBuilder builder = new LmRuntimeBuilder()
				.extractorModelLoader(mock(IExtractorModelLoader.class));
		builder.build();
	}

	@Test
	public void testBuild() {
		LmRuntimeBuilder builder = new LmRuntimeBuilder().extractorModelLoader(
				mock(IExtractorModelLoader.class)).withTargetRuntime(cayenneRuntime);
		LmRuntime runtime = builder.build();
		assertNotNull(runtime);

		ITaskService taskService = runtime.service(ITaskService.class);
		assertNotNull(taskService);
		assertNotNull(taskService.createOrUpdate(CayenneDataObject.class));
	}

	@Test
	public void testAdapter() {

		LinkEtlAdapter adapter = mock(LinkEtlAdapter.class);
		LmRuntimeBuilder builder = new LmRuntimeBuilder().withTargetRuntime(cayenneRuntime).adapter(adapter);

		verifyZeroInteractions(adapter);
		builder.build();
		verify(adapter).contributeToRuntime(any(Binder.class));
	}

}
