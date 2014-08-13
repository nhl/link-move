package com.nhl.link.etl.runtime;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.apache.cayenne.CayenneDataObject;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.junit.Test;

import com.nhl.link.etl.runtime.EtlRuntime;
import com.nhl.link.etl.runtime.EtlRuntimeBuilder;
import com.nhl.link.etl.runtime.extract.IExtractorConfigLoader;
import com.nhl.link.etl.runtime.task.ITaskService;

public class EtlServiceBuilderTest {

	@Test
	public void testBuild_DefaultConfigLoader() {
		EtlRuntimeBuilder builder = new EtlRuntimeBuilder().withTargetRuntime(mock(ServerRuntime.class));
		EtlRuntime runtime = builder.build();

		assertNotNull(runtime);
		ITaskService taskService = runtime.getTaskService();
		assertNotNull(taskService);
		assertNotNull(taskService.createTaskBuilder(CayenneDataObject.class));
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
		assertNotNull(taskService.createTaskBuilder(CayenneDataObject.class));
	}

}
