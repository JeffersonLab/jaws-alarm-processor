package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class LatchRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, RegisteredAlarm> inputTopicRegistered;
    private TestInputTopic<String, ActiveAlarm> inputTopicActive;
    private TestOutputTopic<OverriddenAlarmKey, OverriddenAlarmValue> outputTopic;
    private RegisteredAlarm registered1;
    private RegisteredAlarm registered2;
    private ActiveAlarm active1;
    private ActiveAlarm active2;

    @Before
    public void setup() {
        final LatchRule rule = new LatchRule();

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicRegistered = testDriver.createInputTopic(LatchRule.INPUT_TOPIC_REGISTERED, LatchRule.INPUT_KEY_REGISTERED_SERDE.serializer(), LatchRule.INPUT_VALUE_REGISTERED_SERDE.serializer());
        inputTopicActive = testDriver.createInputTopic(LatchRule.INPUT_TOPIC_ACTIVE, LatchRule.INPUT_KEY_ACTIVE_SERDE.serializer(), LatchRule.INPUT_VALUE_ACTIVE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(LatchRule.OUTPUT_TOPIC, LatchRule.OUTPUT_KEY_SERDE.deserializer(), LatchRule.OUTPUT_VALUE_SERDE.deserializer());

        registered1 = new RegisteredAlarm();
        registered2 = new RegisteredAlarm();

        registered1.setClass$(AlarmClass.Base_Class);
        registered1.setProducer(new SimpleProducer());
        registered1.setLatching(true);

        registered2.setClass$(AlarmClass.Base_Class);
        registered2.setProducer(new SimpleProducer());
        registered2.setLatching(false);

        active1 = new ActiveAlarm();
        active2 = new ActiveAlarm();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void otherOverridesMixedIn() {
        // TODO: make sure processor ignores other overrides
    }

    @Test
    public void tombstoneMsg() {
        // TODO: This auto processor should NOT be doing any acknowledgements (no null payloads)...
    }

    @Test
    public void notLatching() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegistered.pipeInput("alarm1", registered2);
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());

        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = results.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertNull(result.value);
    }

    @Test
    public void latching() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegistered.pipeInput("alarm1", registered1);
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());

        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = results.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertEquals(new OverriddenAlarmValue(new LatchedAlarm()), result.value);
    }
}
