package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.Row;
import com.nhl.link.move.RowAttribute;

import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;

/**
 * @since 1.4
 */
class XmlNodeRow implements Row {
	private final XmlRowAttribute[] attributes;
	private final Node node;

	public XmlNodeRow(XmlRowAttribute[] attributes, Node node) {
		this.attributes = attributes;
		this.node = node;
	}

	@Override
	public Object get(RowAttribute attribute) {
		// TODO: remove cast (maybe Row should have a generic type argument,
		// indicating the type of it's attributes? this will require global refactoring though)
		XmlRowAttribute xmlAttribute = (XmlRowAttribute) attribute;
		try {
			return xmlAttribute.getSourceExpression().evaluate(node);
		} catch (XPathExpressionException e) {
			throw new LmRuntimeException("Failed to evaluate xPath expression: " + attribute.getSourceName(), e);
		}
	}

	@Override
	public RowAttribute[] attributes() {
		return attributes;
	}
}
