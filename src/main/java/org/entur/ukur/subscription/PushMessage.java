package org.entur.ukur.subscription;

import java.io.Serializable;

public class PushMessage implements Serializable {

    private String messagename;
    private String xmlPayload;

    public String getMessagename() {
        return messagename;
    }

    public void setMessagename(String messagename) {
        this.messagename = messagename;
    }

    public String getXmlPayload() {
        return xmlPayload;
    }

    public void setXmlPayload(String xmlPayload) {
        this.xmlPayload = xmlPayload;
    }
}
