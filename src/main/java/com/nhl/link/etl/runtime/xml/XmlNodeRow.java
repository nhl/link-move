package com.nhl.link.etl.runtime.xml;

import com.nhl.link.etl.Row;
import com.nhl.link.etl.RowAttribute;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @since 1.4
 */
class XmlNodeRow implements Row {
	private final RowAttribute[] attributes;
	private final Node node;

	public XmlNodeRow(RowAttribute[] attributes, Node node) {
		this.attributes = attributes;
		this.node = node;
	}

	@Override
	public Object get(RowAttribute attribute) {
		NodeList childNodes = node.getChildNodes();
		for (int i = 0; i <= childNodes.getLength(); i++) {
			Node childNode = childNodes.item(i);
			if (childNode.getNodeName().equals(attribute.getSourceName())) {
				return childNode.getTextContent();
			}
		}
		return null;
	}

	@Override
	public RowAttribute[] attributes() {
		return attributes;
	}
}
