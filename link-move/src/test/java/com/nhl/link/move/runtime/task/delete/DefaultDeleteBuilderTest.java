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
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultDeleteBuilderTest {

	private DefaultDeleteBuilder<Etl1t> builder;

	@BeforeEach
	public void before() {

		ObjAttribute matchAttribute = new ObjAttribute("abc");
		matchAttribute.setType(Object.class.getName());

		ObjEntity targetEntity = new ObjEntity();
		targetEntity.addAttribute(matchAttribute);

		EntityResolver resolver = mock(EntityResolver.class);
		when(resolver.getObjEntity(any(Class.class))).thenReturn(targetEntity);

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
								mock(ITokenManager.class),
								keyAdapterFactory,
                                mock(ValueConverterFactory.class),
								mock(LmLogger.class)));

		MapperBuilder mapperBuilder = new MapperBuilder(targetEntity, mockTargetEntity, keyAdapterFactory);

		this.builder = new DefaultDeleteBuilder<>(Etl1t.class,
				cayenneService,
				mock(ITokenManager.class),
				taskService,
                mapperBuilder,
				mock(LmLogger.class));
	}

	@Test
	public void testTask_NoExtractorName() {
		assertThrows(IllegalStateException.class, () -> builder.task());
	}

}
