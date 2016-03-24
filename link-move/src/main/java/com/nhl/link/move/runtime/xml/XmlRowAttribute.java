package com.nhl.link.move.runtime.xml;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public class XmlRowAttribute implements RowAttribute {

    private RowAttribute attribute;
    private XPathExpression sourceExpression;

    public XmlRowAttribute(RowAttribute attribute, XPathFactory xPathFactory) {
        this.attribute = attribute;
        try {
            this.sourceExpression = xPathFactory.newXPath().compile(attribute.getSourceName());
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
