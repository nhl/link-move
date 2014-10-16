package com.nhl.link.etl.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.parser.ASTIn;
import org.apache.cayenne.exp.parser.ASTList;
import org.apache.cayenne.exp.parser.ASTPath;
import org.apache.cayenne.query.SelectQuery;

public abstract class CayenneMatcherTest extends BaseMatcherTest<DataObject> {

	protected void checkInExpression(SelectQuery<DataObject> query, Class<? extends ASTPath> pathClass) {
		assertEquals(ASTIn.class, query.getQualifier().getClass());

		ASTIn expression = (ASTIn) query.getQualifier();
		assertNotNull(expression.jjtGetChild(0));
		assertEquals(pathClass, expression.jjtGetChild(0).getClass());
		assertEquals(SOURCE_KEY, ((ASTPath) expression.jjtGetChild(0)).getPath());
		assertNotNull(expression.jjtGetChild(1));
		assertEquals(ASTList.class, expression.jjtGetChild(1).getClass());
		Object[] values = (Object[]) ((ASTList) expression.jjtGetChild(1)).evaluate(null);

		Set<Object> sourceValues = new HashSet<>();
		for (Map<String, Object> source : sources) {
			sourceValues.add(source.get(SOURCE_KEY));
		}
		assertEquals(sourceValues.size(), values.length);
		for (Object value : values) {
			assertTrue(sourceValues.contains(value));
		}
	}
}
