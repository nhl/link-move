package com.nhl.link.framework.etl.transform;

import junit.framework.Assert;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.exp.parser.ASTIn;
import org.apache.cayenne.exp.parser.ASTList;
import org.apache.cayenne.exp.parser.ASTPath;
import org.apache.cayenne.query.SelectQuery;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public abstract class CayenneMatcherTest extends BaseMatcherTest<DataObject> {

	protected void checkInExpression(SelectQuery<DataObject> query, Class<? extends ASTPath> pathClass) {
		Assert.assertEquals(ASTIn.class, query.getQualifier().getClass());

		ASTIn expression = (ASTIn) query.getQualifier();
		assertNotNull(expression.jjtGetChild(0));
		Assert.assertEquals(pathClass, expression.jjtGetChild(0).getClass());
		Assert.assertEquals(SOURCE_KEY, ((ASTPath) expression.jjtGetChild(0)).getPath());
		assertNotNull(expression.jjtGetChild(1));
		Assert.assertEquals(ASTList.class, expression.jjtGetChild(1).getClass());
		Object[] values = (Object[]) ((ASTList) expression.jjtGetChild(1)).evaluate(null);

		Set<Object> sourceValues = new HashSet<>();
		for (Map<String, Object> source : sources) {
			sourceValues.add(source.get(SOURCE_KEY));
		}
		Assert.assertEquals(sourceValues.size(), values.length);
		for (Object value : values) {
			assertTrue(sourceValues.contains(value));
		}
	}
}
