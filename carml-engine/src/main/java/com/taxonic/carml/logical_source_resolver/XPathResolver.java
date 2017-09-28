package com.taxonic.carml.logical_source_resolver;


import com.sun.xml.internal.ws.util.xml.NodeListIterator;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Optional;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public class XPathResolver implements LogicalSourceResolver<Node> {

	private XPath xpath = XPathFactory.newInstance().newXPath();

	@Override
	public SourceIterator<Node> getSourceIterator() {
		return (inputStream, iteratorExpression) -> {
			try {

				DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
				Document doc = documentBuilder.parse(inputStream);
				Object result = xpath.evaluate(iteratorExpression, doc, XPathConstants.NODESET);

				return () -> new NodeListIterator((NodeList) result);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		};
	}

	@Override
	public ExpressionEvaluatorFactory<Node> getExpressionEvaluatorFactory() {
		return entry -> expression -> {
			try {
				String result= xpath.evaluate(expression, entry);
				return Optional.ofNullable(result);
			} catch (XPathExpressionException e) {
				throw new RuntimeException(e);
			}
		};
	}

}
