package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;
import java.util.Iterator;

/**
 * @since 1.4
 */
public class XmlRowReader implements RowReader {

    private final XmlRowAttribute[] header;
    private final NodeList nodes;

    public XmlRowReader(XmlRowAttribute[] header, NodeList nodes) {
        this.header = header;
        this.nodes = nodes;
    }

    @Override
    public RowAttribute[] getHeader() {
        return header;
    }

    @Override
    public void close() {
        // no need to close anything
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new Iterator<Object[]>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < nodes.getLength();
            }

            @Override
            public Object[] next() {
                return fromNode(nodes.item(i++));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Object[] fromNode(Node node) {

        Object[] row = new Object[header.length];

        for (int i = 0; i < header.length; i++) {
            row[i] = valueFromNode(header[i], node);
        }

        return row;
    }

    private Object valueFromNode(XmlRowAttribute attribute, Node node) {
        try {
            return attribute.getSourceExpression().evaluate(node);
        } catch (XPathExpressionException e) {
            throw new LmRuntimeException("Failed to evaluate xPath expression: " + attribute.getSourceName(), e);
        }
    }
}
