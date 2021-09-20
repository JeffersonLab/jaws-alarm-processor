package org.jlab.jaws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


public class AutoOverrideProcessor {
    private static final Logger log = LoggerFactory.getLogger(AutoOverrideProcessor.class);
    private static final Set<AutoOverrideRule> rules = new HashSet<>();

    /**
     * Entrypoint of the application.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {

        rules.add(new MonologRule());
        rules.add(new ShelveExpirationRule());
        rules.add(new LatchRule());
        rules.add(new OneShotRule());

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                for(AutoOverrideRule rule: rules) {
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
            for(AutoOverrideRule rule: rules) {
                rule.start();
            }
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
