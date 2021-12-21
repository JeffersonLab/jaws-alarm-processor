package org.jlab.jaws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


public class EffectiveProcessor {
    private static final Logger log = LoggerFactory.getLogger(EffectiveProcessor.class);
    private static final Set<ProcessingRule> rules = new HashSet<>();

    /**
     * Entrypoint of the application.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {

        // async
        rules.add(new ShelveExpirationRule("alarm-overrides", "alarm-overrides"));

        // pipelined
        rules.add(new RegistrationRule("alarm-classes", "alarm-instances", "effective-registrations", "intermediate-registration"));
        rules.add(new ActivationRule("intermediate-registration", "alarm-activations", "alarm-overrides", "intermediate-activation"));
        rules.add(new LatchRule("intermediate-activation", "intermediate-latch", "alarm-overrides"));
        rules.add(new OneShotRule("intermediate-latch", "intermediate-oneshot", "alarm-overrides"));
        rules.add(new EffectiveStateRule("intermediate-oneshot", "effective-activations", "effective-alarms"));

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                for(ProcessingRule rule: rules) {
                    try {
                        rule.close();
                    } catch(Exception e) {
                        log.warn("Unable to close rule", e);
                    }
                }
                latch.countDown();
            }
        });

        try {
            for(ProcessingRule rule: rules) {
                rule.start();
            }
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
