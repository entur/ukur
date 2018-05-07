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

package org.entur.ukur.setup;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UkurConfiguration {


    private static final String QUEUE_PREFIX = "ukur";
    public static final String ET_QUEUE = QUEUE_PREFIX + ".et?timeToLive=300000&disableReplyTo=true"; //5 minutes time to live
    public static final String SX_QUEUE = QUEUE_PREFIX + ".sx?timeToLive=900000&disableReplyTo=true"; //30 minutes time to live
    public static final String SUB_SX_QUEUE = QUEUE_PREFIX + ".sub_sx?timeToLive=900000&disableReplyTo=true"; //30 minutes time to live
    public static final String SUB_ET_QUEUE = QUEUE_PREFIX + ".sub_et?timeToLive=900000&disableReplyTo=true"; //30 minutes time to live

    @Value("${rutebanken.kubernetes.url:}")
    private String kubernetesUrl;

    @Value("${rutebanken.kubernetes.enabled:true}")
    private boolean kubernetesEnabled;

    @Value("${rutebanken.kubernetes.namespace:default}")
    private String namespace;

    @Value("${rutebanken.hazelcast.management.url:}")
    private String hazelcastManagementUrl;

    @Value("${ukur.camel.anshar.url}")
    private String ansharURL;

    @Value("${ukur.camel.anshar.subscriptionPostfix:/subscribe}")
    private String ansharSubscriptionPostfix;

    @Value("${ukur.camel.anshar.receiver.baseurl}")
    private String ownSubscriptionURL;

    @Value("${ukur.camel.anshar.et.enabled}")
    private boolean etEnabled;

    @Value("${ukur.camel.anshar.sx.enabled}")
    private boolean sxEnabled;

    @Value("${ukur.camel.anshar.subscription:false}")
    private boolean useAnsharSubscription;

    @Value("${ukur.camel.rest.port}")
    private int restPort;

    @Value("${ukur.camel.polling.interval}")
    private int pollingInterval;

    @Value("${ukur.camel.tiamat.stop_place_quays.url}")
    private String tiamatStopPlaceQuaysURL;

    @Value("${ukur.camel.tiamat.stop_place_quays.interval}")
    private int tiamatStopPlaceQuaysInterval;

    @Value("${ukur.camel.tiamat.stop_place_quays.enabled}")
    private boolean tiamatStopPlaceQuaysEnabled;

    @Value("${ukur.camel.anshar.subscription.checking:true}")
    private boolean subscriptionCheckingEnabled;

    public String getHazelcastManagementUrl() {
        return hazelcastManagementUrl;
    }

    public String getKubernetesUrl() {
        return kubernetesUrl;
    }

    public boolean isKubernetesEnabled() {
        return kubernetesEnabled;
    }

    public String getNamespace() {
        return namespace;
    }

    private String getAnsharURL() {
        if (ansharURL != null) {
            String url = ansharURL.trim();
            if (url.endsWith("/")) {
                return url.substring(0, url.length() - 1);
            }
            return url;
        }
        return null;
    }

    private String getAnsharSubscriptionPostfix() {
        if (!ansharSubscriptionPostfix.startsWith("/")) {
            return "/"+ansharSubscriptionPostfix;
        }
        return ansharSubscriptionPostfix;
    }

    public String getAnsharETCamelUrl(String requestorId) {
        return getAnsharURL() + "/rest/et?requestorId=" + requestorId + "&maxSize=500";
    }

    public String getAnsharSXCamelUrl(String requestorId) {
        return getAnsharURL() + "/rest/sx?requestorId=" + requestorId + "&maxSize=500";
    }

    public String getAnsharSubscriptionUrl() {
        return getAnsharURL() + getAnsharSubscriptionPostfix();
    }

    public String getOwnSubscriptionURL() {
        if (ownSubscriptionURL == null) {
            //not to be called if not set
            throw new IllegalStateException("Required (when subscribing to anshar) config for own subscription url is not set");
        }
        String trimmed = ownSubscriptionURL.trim();
        if (!trimmed.endsWith("/")) {
            return trimmed + "/";
        }
        return trimmed;
    }

    public boolean isEtEnabled() {
        return etEnabled;
    }

    public boolean isSxEnabled() {
        return sxEnabled;
    }

    public int getRestPort() {
        return restPort;
    }

    public int getPollingInterval() {
        return pollingInterval;
    }

    public String getTiamatStopPlaceQuaysURL() {
        return tiamatStopPlaceQuaysURL;
    }

    public int getTiamatStopPlaceQuaysInterval() {
        return tiamatStopPlaceQuaysInterval;
    }

    public boolean isTiamatStopPlaceQuaysEnabled() {
        return tiamatStopPlaceQuaysEnabled;
    }

    public boolean useAnsharSubscription() {
        return useAnsharSubscription;
    }

    public boolean isSubscriptionCheckingEnabled() {
        return subscriptionCheckingEnabled;
    }

    public void setSubscriptionCheckingEnabled(boolean subscriptionCheckingEnabled) {
        this.subscriptionCheckingEnabled = subscriptionCheckingEnabled;
    }
}
