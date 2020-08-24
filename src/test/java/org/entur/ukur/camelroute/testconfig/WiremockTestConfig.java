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

package org.entur.ukur.camelroute.testconfig;

import org.entur.ukur.setup.UkurConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.io.IOException;
import java.net.ServerSocket;

@Configuration
@Primary
public class WiremockTestConfig extends UkurConfiguration {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${wiremock.server.port}")
    private String wiremockPort;
    private Integer restPort = null;

    @Override
    public int getRestPort() {
        if (restPort == null) {
            try (ServerSocket s = new ServerSocket(0)) {
                int localPort = s.getLocalPort();
                logger.info("Use {} as REST port", localPort);
                restPort = localPort;
            } catch (IOException e) {
                throw new RuntimeException("Could not get an available port...", e);
            }
        }
        return  restPort;
    }

    public String getWiremockPort() {
        return wiremockPort;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }


}
