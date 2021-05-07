package org.apache.pulsar;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.mockito.Mockito;

public class SmockTests {

    public static void main(String[] args) throws PulsarServerException {
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        PulsarService pulsarService = Mockito.spy(new PulsarService(serviceConfiguration));
        pulsarService.start();
    }

}
