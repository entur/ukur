/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *  https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */

package org.entur.ukur.xml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Iterator;
import java.util.Stack;

/**
 * Writes XML without any namespace references and with pretty printing 
 * copied from com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter.
 */
public class NoNamespaceIndentingXMLStreamWriter extends DelegatingXMLStreamWriter {

    private static final Object SEEN_NOTHING = new Object();
    private static final Object SEEN_ELEMENT = new Object();
    private static final Object SEEN_DATA = new Object();
    private static final NamespaceContext emptyNamespaceContext = new NamespaceContext() {
        @Override
        public String getNamespaceURI(String prefix) {
            return "";
        }

        @Override
        public String getPrefix(String namespaceURI) {
            return "";
        }

        @Override
        public Iterator getPrefixes(String namespaceURI) {
            return null;
        }
    };
    private Object state;
    private Stack<Object> stateStack;
    private String indentStep;
    private int depth;

    public NoNamespaceIndentingXMLStreamWriter(XMLStreamWriter writer) {
        super(writer);
        state = SEEN_NOTHING;
        stateStack = new Stack<>();
        indentStep = "  ";
        depth = 0;
    }

    @Override
    public NamespaceContext getNamespaceContext() {
        //no namespace...
        return emptyNamespaceContext;
    }

    @Override
    public void writeNamespace(String prefix, String namespaceURI) {
        //no namespace...
    }
    
    private void onStartElement() throws XMLStreamException {
        stateStack.push(SEEN_ELEMENT);
        state = SEEN_NOTHING;
        if (depth > 0) {
            super.writeCharacters("\n");
        }
        doIndent();
        ++depth;
    }

    private void onEndElement() throws XMLStreamException {
        --depth;
        if (state == SEEN_ELEMENT) {
            super.writeCharacters("\n");
            doIndent();
        }
        state = stateStack.pop();
    }

    private void onEmptyElement() throws XMLStreamException {
        state = SEEN_ELEMENT;
        if (depth > 0) {
            super.writeCharacters("\n");
        }
        doIndent();
    }

    private void doIndent() throws XMLStreamException {
        if (depth > 0) {
            for(int i = 0; i < depth; ++i) {
                super.writeCharacters(indentStep);
            }
        }
    }

    @Override
    public void writeStartDocument() throws XMLStreamException {
        super.writeStartDocument();
        super.writeCharacters("\n");
    }

    @Override
    public void writeStartDocument(String version) throws XMLStreamException {
        super.writeStartDocument(version);
        super.writeCharacters("\n");
    }

    @Override
    public void writeStartDocument(String encoding, String version) throws XMLStreamException {
        super.writeStartDocument(encoding, version);
        super.writeCharacters("\n");
    }

    @Override
    public void writeStartElement(String localName) throws XMLStreamException {
        onStartElement();
        super.writeStartElement(localName);
    }

    @Override
    public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException {
        onStartElement();
        super.writeStartElement(namespaceURI, localName);
    }

    @Override
    public void writeStartElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
        onStartElement();
        super.writeStartElement(prefix, localName, namespaceURI);
    }

    @Override
    public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
        onEmptyElement();
        super.writeEmptyElement(namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
        onEmptyElement();
        super.writeEmptyElement(prefix, localName, namespaceURI);
    }

    @Override
    public void writeEmptyElement(String localName) throws XMLStreamException {
        onEmptyElement();
        super.writeEmptyElement(localName);
    }

    @Override
    public void writeEndElement() throws XMLStreamException {
        onEndElement();
        super.writeEndElement();
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
        state = SEEN_DATA;
        super.writeCharacters(text);
    }

    @Override
    public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
        state = SEEN_DATA;
        super.writeCharacters(text, start, len);
    }

    @Override
    public void writeCData(String data) throws XMLStreamException {
        state = SEEN_DATA;
        super.writeCData(data);
    }

}
