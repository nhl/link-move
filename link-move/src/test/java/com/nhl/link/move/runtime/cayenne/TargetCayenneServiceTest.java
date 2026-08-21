package com.nhl.link.move.runtime.cayenne;

import org.apache.cayenne.access.DataDomain;
import org.apache.cayenne.access.DataNode;
import org.apache.cayenne.runtime.CayenneRuntime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TargetCayenneServiceTest {

	private DataSource ds1;
	private DataSource ds2;
	private TargetCayenneService cayenneService;

	@BeforeEach
	public void before() {

		ds1 = mock(DataSource.class);
		ds2 = mock(DataSource.class);

		DataNode dn1 = mock(DataNode.class);
		when(dn1.getName()).thenReturn("dn1");
		when(dn1.getDataSource()).thenReturn(ds1);

		DataNode dn2 = mock(DataNode.class);
		when(dn2.getName()).thenReturn("dn2");
		when(dn2.getDataSource()).thenReturn(ds2);

		DataDomain domain = mock(DataDomain.class);
		when(domain.getDataNodes()).thenReturn(List.of(dn1, dn2));

		CayenneRuntime runtime = mock(CayenneRuntime.class);
		when(runtime.getDataDomain()).thenReturn(domain);

		cayenneService = new TargetCayenneService(runtime);
	}

	@Test
	public void dataSources() {
		Map<String, DataSource> dss = cayenneService.dataSources();
		assertEquals(2, dss.size());
		assertSame(ds1, dss.get("dn1"));
		assertSame(ds2, dss.get("dn2"));
	}
}
