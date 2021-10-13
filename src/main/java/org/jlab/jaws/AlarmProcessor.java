package org.jlab.jaws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


public class AlarmProcessor {
    private static final Logger log = LoggerFactory.getLogger(AlarmProcessor.class);
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
        rules.add(new MonologRule("alarm-classes", "alarm-registrations", "alarm-activations", "alarm-overrides", "intermediate-monolog"));
        rules.add(new LatchRule("intermediate-monolog", "intermediate-latch-processed", "alarm-overrides"));
        rules.add(new OneShotRule("intermediate-latch-processed", "intermediate-unshelve-processed", "alarm-overrides"));
        rules.add(new EffectiveStateRule("intermediate-unshelve-processed", "alarms"));

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
