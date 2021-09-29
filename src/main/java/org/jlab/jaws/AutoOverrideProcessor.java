package org.jlab.jaws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


public class AutoOverrideProcessor {
    private static final Logger log = LoggerFactory.getLogger(AutoOverrideProcessor.class);
    private static final Set<ProcessingRule> rules = new HashSet<>();

    /**
     * Entrypoint of the application.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {

        // async
        rules.add(new ShelveExpirationRule("overridden-alarms", "overridden-alarms"));

        // pipelined
        rules.add(new MonologRule("registered-classes", "registered-alarms", "active-alarms", "overridden-alarms", "monolog"));
        rules.add(new LatchRule("monolog", "latch-processed", "overridden-alarms"));
        rules.add(new OneShotRule("latch-processed", "unshelve-processed", "overridden-alarms"));
        rules.add(new EffectiveStateRule("unshelve-processed", "alarms"));

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
