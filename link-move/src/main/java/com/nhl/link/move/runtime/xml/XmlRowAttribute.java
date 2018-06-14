package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;

import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public class XmlRowAttribute implements RowAttribute {

    private RowAttribute attribute;
    private XPathExpression sourceExpression;

    public XmlRowAttribute(RowAttribute attribute, XPathFactory xPathFactory) {
        this(attribute, xPathFactory, null);
    }

    public XmlRowAttribute(RowAttribute attribute, XPathFactory xPathFactory, NamespaceContext namespaceContext) {
        this.attribute = attribute;
        try {
            XPath xPath = xPathFactory.newXPath();
            if (namespaceContext != null) {
                xPath.setNamespaceContext(namespaceContext);
            }
            this.sourceExpression = xPath.compile(attribute.getSourceName());
        } catch (XPathExpressionException e) {
            throw new LmRuntimeException("Invalid xPath expression: " + attribute.getSourceName(), e);
        }
    }

    @Override
    public int getOrdinal() {
        return attribute.getOrdinal();
    }

    @Override
    public Class<?> type() {
        return attribute.type();
    }

    @Override
    public String getSourceName() {
        return attribute.getSourceName();
    }

    @Override
    public String getTargetPath() {
        return attribute.getTargetPath();
    }

    public XPathExpression getSourceExpression() {
        return sourceExpression;
    }
}
