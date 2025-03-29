package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.targetmodel.TargetEntityMap;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.sourcekeys.DefaultSourceKeysBuilder;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultDeleteBuilderTest {

	private DefaultDeleteBuilder builder;

	@BeforeEach
	public void before() {

		DataMap dataMap = new DataMap();
		DbEntity dbTargetEntity = new DbEntity("_e1");
		dataMap.addDbEntity(dbTargetEntity);
		DbAttribute pk = new DbAttribute("pk", Types.BIGINT, dbTargetEntity);
		pk.setPrimaryKey(true);
		dbTargetEntity.addAttribute(pk);

		ObjAttribute matchAttribute = new ObjAttribute("abc");
		matchAttribute.setType(Object.class.getName());

		ObjEntity targetEntity = new ObjEntity("e1");
		dataMap.addObjEntity(targetEntity);

		targetEntity.setDbEntity(dbTargetEntity);
		targetEntity.addAttribute(matchAttribute);

		EntityResolver resolver = new EntityResolver(List.of(dataMap));
		
		ITargetCayenneService cayenneService = mock(ITargetCayenneService.class);
		when(cayenneService.entityResolver()).thenReturn(resolver);

		IKeyAdapterFactory keyAdapterFactory = mock(IKeyAdapterFactory.class);

		TargetEntity mockTargetEntity = mock(TargetEntity.class);
		when(mockTargetEntity.getAttribute(any(String.class))).thenReturn(Optional.empty());

		TargetEntityMap mockPathNormalizer = mock(TargetEntityMap.class);
		when(mockPathNormalizer.get(targetEntity)).thenReturn(mockTargetEntity);

		ITaskService taskService = mock(ITaskService.class);
		when(taskService.extractSourceKeys(Etl1t.class)).thenReturn(
						new DefaultSourceKeysBuilder(
                                mockTargetEntity,
								mock(IExtractorService.class),
								keyAdapterFactory,
                                mock(ValueConverterFactory.class),
								mock(LmLogger.class)));

		MapperBuilder mapperBuilder = new MapperBuilder(targetEntity, mockTargetEntity, keyAdapterFactory);

		this.builder = new DefaultDeleteBuilder(Etl1t.class,
				cayenneService,
				taskService,
                mapperBuilder,
				mock(LmLogger.class));
	}

	@Test
	public void task_NoExtractorName() {
		assertThrows(IllegalStateException.class, () -> builder.task());
	}

}
