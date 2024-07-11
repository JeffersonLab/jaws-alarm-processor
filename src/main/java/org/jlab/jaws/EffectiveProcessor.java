package org.jlab.jaws;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import org.jlab.jaws.clients.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    rules.add(new ShelveExpirationRule(OverrideProducer.TOPIC, OverrideProducer.TOPIC));

    // pipelined
    rules.add(
        new RegistrationRule(
            ClassProducer.TOPIC,
            InstanceProducer.TOPIC,
            EffectiveRegistrationProducer.TOPIC,
            "intermediate-registration"));
    rules.add(
        new ActivationRule(
            "intermediate-registration",
            ActivationProducer.TOPIC,
            OverrideProducer.TOPIC,
            "intermediate-activation"));
    rules.add(
        new LatchRule("intermediate-activation", "intermediate-latch", OverrideProducer.TOPIC));
    rules.add(
        new OnDelayRule("intermediate-latch", "intermediate-ondelay", OverrideProducer.TOPIC));
    rules.add(
        new OneShotRule("intermediate-ondelay", "intermediate-oneshot", OverrideProducer.TOPIC));
    rules.add(
        new EffectiveStateRule(
            "intermediate-oneshot",
            EffectiveNotificationProducer.TOPIC,
            EffectiveAlarmProducer.TOPIC));

    final CountDownLatch latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                for (ProcessingRule rule : rules) {
                  try {
                    rule.close();
                  } catch (Exception e) {
                    log.warn("Unable to close rule", e);
                  }
                }
                latch.countDown();
              }
            });

    try {
      for (ProcessingRule rule : rules) {
        rule.start();
      }
      latch.await();
    } catch (final Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }
}
