package org.entur.ukur.subscription;

import com.google.common.base.Objects;

import java.io.Serializable;

public class MessageIdentifierKey implements Serializable {
    final String subscriptionId, messageIdentifier;

    private int hashCode = -1;

    MessageIdentifierKey(String subscriptionId, String messageIdentifier) {
        this.subscriptionId = subscriptionId;
        this.messageIdentifier = messageIdentifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageIdentifierKey that = (MessageIdentifierKey) o;
        return Objects.equal(subscriptionId, that.subscriptionId) &&
                Objects.equal(messageIdentifier, that.messageIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(subscriptionId, messageIdentifier);
    }
}
