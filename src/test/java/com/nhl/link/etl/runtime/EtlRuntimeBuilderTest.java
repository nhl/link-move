package com.nhl.link.etl.runtime;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.cayenne.CayenneDataObject;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.di.Binder;
import org.junit.Test;

import com.nhl.link.etl.runtime.adapter.LinkEtlAdapter;
import com.nhl.link.etl.runtime.extract.IExtractorConfigLoader;
import com.nhl.link.etl.runtime.task.ITaskService;

public class EtlRuntimeBuilderTest {

	@Test
	public void testBuild_DefaultConfigLoader() {
		EtlRuntimeBuilder builder = new EtlRuntimeBuilder().withTargetRuntime(mock(ServerRuntime.class));
		EtlRuntime runtime = builder.build();

		assertNotNull(runtime);
		ITaskService taskService = runtime.getTaskService();
		assertNotNull(taskService);
		assertNotNull(taskService.createOrUpdate(CayenneDataObject.class));
	}

	@Test(expected = IllegalStateException.class)
	public void testBuild_NoTargetRuntime() {
		EtlRuntimeBuilder builder = new EtlRuntimeBuilder()
				.withExtractorConfigLoader(mock(IExtractorConfigLoader.class));
		builder.build();
	}

	@Test
	public void testBuild() {
		EtlRuntimeBuilder builder = new EtlRuntimeBuilder().withExtractorConfigLoader(
				mock(IExtractorConfigLoader.class)).withTargetRuntime(mock(ServerRuntime.class));
		EtlRuntime runtime = builder.build();
		assertNotNull(runtime);

		ITaskService taskService = runtime.getTaskService();
		assertNotNull(taskService);
		assertNotNull(taskService.createOrUpdate(CayenneDataObject.class));
	}

	@Test
	public void testAdapter() {

		LinkEtlAdapter adapter = mock(LinkEtlAdapter.class);
		EtlRuntimeBuilder builder = new EtlRuntimeBuilder().withTargetRuntime(mock(ServerRuntime.class)).adapter(
				adapter);

		verifyZeroInteractions(adapter);
		builder.build();
		verify(adapter).contributeToRuntime(any(Binder.class));
	}

}
