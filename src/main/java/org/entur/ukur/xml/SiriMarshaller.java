package org.entur.ukur.xml;

import org.springframework.stereotype.Component;
import uk.org.siri.siri20.Siri;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.*;
import java.io.InputStream;
import java.io.StringWriter;

@Component
public class SiriMarshaller {

    private final JAXBContext jaxbContext;

    public SiriMarshaller() throws JAXBException {
        jaxbContext = JAXBContext.newInstance(Siri.class);
    }

    public <T> T unmarhall(InputStream xml, Class<T>resultingClass) throws JAXBException, XMLStreamException {
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        XMLInputFactory xmlif = XMLInputFactory.newInstance();
        XMLStreamReader xmlsr = xmlif.createXMLStreamReader(xml);
        return resultingClass.cast(jaxbUnmarshaller.unmarshal(xmlsr));
    }

    public String prettyPrintNoNamespaces(Object element) throws JAXBException, XMLStreamException {
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
        StringWriter stringWriter = new StringWriter();
        XMLStreamWriter writer = XMLOutputFactory.newFactory().createXMLStreamWriter(stringWriter);
        jaxbMarshaller.marshal(element, new NoNamespaceIndentingXMLStreamWriter(writer));
        return stringWriter.getBuffer().toString();
    }

}
