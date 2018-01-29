package org.entur.ukur.route;

import org.entur.ukur.setup.UkurConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.IOException;
import java.net.ServerSocket;

@Configuration
@Primary
public class WiremockTestConfig extends UkurConfiguration {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${wiremock.server.port}")
    private String wiremockPort;

    @Override
    public String getAnsharETCamelUrl(String uuid) {
        return "http4://localhost:" + wiremockPort + "/et";
    }

    @Override
    public String getAnsharSXCamelUrl(String uuid) {
        return "http4://localhost:" + wiremockPort + "/sx";
    }

    @Override
    public boolean isEtPollingEnabled() {
        return false;
    }

    @Override
    public boolean isSxPollingEnabled() {
        return false;
    }

    @Override
    public int getRestPort() {
        try (ServerSocket s = new ServerSocket(0)){
            int localPort = s.getLocalPort();
            logger.info("Use {} as REST port", localPort);
            return localPort;
        } catch (IOException e) {
            throw new RuntimeException("Could not get an available port...", e);
        }
    }

    public String getWiremockPort() {
        return wiremockPort;
    }
}
