/*
 * Copyright 2015 Santhosh Kumar Tekuri
 *
 * The JLibs authors license this file to you under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * Modifications copyright (C) 2022 Skemu
 */

package com.taxonic.carml.logicalsourceresolver;

import static javax.xml.stream.XMLStreamConstants.CDATA;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.COMMENT;
import static javax.xml.stream.XMLStreamConstants.DTD;
import static javax.xml.stream.XMLStreamConstants.END_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.PROCESSING_INSTRUCTION;
import static javax.xml.stream.XMLStreamConstants.SPACE;
import static javax.xml.stream.XMLStreamConstants.START_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import jlibs.xml.sax.AbstractXMLReader;
import jlibs.xml.stream.STAXAttributes;
import jlibs.xml.stream.STAXLocator;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class PausableStaxXmlReader extends AbstractXMLReader {
  private final XMLInputFactory factory;

  private final AtomicBoolean readingPaused = new AtomicBoolean();

  private boolean readingCompleted = false;

  private XMLStreamReader reader = null;

  private Attributes attrs = null;

  public PausableStaxXmlReader(XMLInputFactory factory) {
    this.factory = factory;
  }

  public PausableStaxXmlReader() {
    this(XMLInputFactory.newInstance());
  }

  @Override
  public void parse(InputSource input) throws SAXException {
    try {
      if (input.getByteStream() != null) {
        reader = factory.createXMLStreamReader(input.getByteStream(), input.getEncoding());
      } else if (input.getCharacterStream() != null) {
        reader = factory.createXMLStreamReader(input.getCharacterStream());
      } else {
        reader = factory.createXMLStreamReader(input.getSystemId(), (InputStream) null);
      }

      attrs = new STAXAttributes(reader);
      start();
    } catch (XMLStreamException ex) {
      throw new SAXException(ex);
    }
  }

  @Override
  public void parse(String systemId) throws SAXException {
    parse(new InputSource(systemId));
  }

  public boolean isPaused() {
    return readingPaused.get();
  }

  public boolean isCompleted() {
    return readingCompleted;
  }

  public void pause() {
    readingPaused.compareAndSet(false, true);
  }

  private void start() throws SAXException, XMLStreamException {
    int eventType = reader.getEventType();
    if (!(eventType == START_DOCUMENT || eventType == START_ELEMENT)) {
      throw new IllegalStateException("XMLStreamReader not at start of document or element");
    }
    run(eventType);
  }

  public void resume() throws SAXException, XMLStreamException {
    readingPaused.compareAndSet(true, false);
    int eventType = reader.next();
    run(eventType);
  }

  private void run(int eventType) throws SAXException, XMLStreamException {
    while (true) {
      switch (eventType) {
        case START_ELEMENT: {
          handleStartElement();
          break;
        }
        case END_ELEMENT: {
          handleEndElement();
          break;
        }
        case PROCESSING_INSTRUCTION:
          handleProcessingInstruction();
          break;
        case CHARACTERS:
        case SPACE:
        case CDATA:
          handleCharacters();
          break;
        case COMMENT:
          handleComment();
          break;
        case START_DOCUMENT:
          handleStartDocument();
          break;
        case END_DOCUMENT:
          handleEndDocument();
          return;
        case DTD:
          handleDtd();
          break;
        default:
          // not interested in event type
      }
      try {
        if (readingPaused.get()) {
          break;
        }
        eventType = reader.next();
      } catch (XMLStreamException ex) {
        throw new SAXException(ex);
      }
    }
  }

  private void handleStartElement() throws SAXException {
    int nsCount = reader.getNamespaceCount();
    for (int i = 0; i < nsCount; i++) {
      String prefix = reader.getNamespacePrefix(i);
      String uri = reader.getNamespaceURI(i);
      handler.startPrefixMapping(prefix == null ? "" : prefix, uri == null ? "" : uri);
    }

    String localName = reader.getLocalName();
    String prefix = reader.getPrefix();
    String qname = prefix == null || prefix.length() == 0 ? localName : prefix + ':' + localName;
    String uri = reader.getNamespaceURI();
    handler.startElement(uri == null ? "" : uri, localName, qname, attrs);
  }

  private void handleEndElement() throws SAXException {
    String localName = reader.getLocalName();
    String prefix = reader.getPrefix();
    String qname = prefix == null || prefix.length() == 0 ? localName : prefix + ':' + localName;
    String uri = reader.getNamespaceURI();
    handler.endElement(uri == null ? "" : uri, localName, qname);

    int nsCount = reader.getNamespaceCount();
    for (int i = 0; i < nsCount; i++) {
      prefix = reader.getNamespacePrefix(i);
      handler.endPrefixMapping(prefix == null ? "" : prefix);
    }
  }

  private void handleProcessingInstruction() throws SAXException {
    handler.processingInstruction(reader.getPITarget(), reader.getPIData());
  }

  private void handleCharacters() throws SAXException {
    handler.characters(reader.getTextCharacters(), reader.getTextStart(), reader.getTextLength());
  }

  private void handleComment() throws SAXException {
    handler.comment(reader.getTextCharacters(), reader.getTextStart(), reader.getTextLength());
  }

  private void handleStartDocument() throws SAXException {
    handler.setDocumentLocator(new STAXLocator(reader));
    handler.startDocument();
  }

  private void handleEndDocument() throws SAXException, XMLStreamException {
    handler.endDocument();
    reader.close();
    readingCompleted = true;
  }

  private void handleDtd() throws SAXException {
    var location = reader.getLocation();
    handler.startDTD(null, location.getPublicId(), location.getSystemId());
    handler.endDTD();
  }
}
