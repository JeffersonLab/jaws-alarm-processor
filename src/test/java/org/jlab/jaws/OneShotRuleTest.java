package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class OneShotRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<OverriddenAlarmKey, OverriddenAlarmValue> inputTopicOverridden;
    private TestInputTopic<String, ActiveAlarm> inputTopicActive;
    private TestOutputTopic<OverriddenAlarmKey, OverriddenAlarmValue> outputTopic;
    private OverriddenAlarmValue overriddenAlarmValue1;
    private OverriddenAlarmValue overriddenAlarmValue2;
    private ActiveAlarm active1;
    private ActiveAlarm active2;

    @Before
    public void setup() {
        final OneShotRule rule = new OneShotRule();

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicOverridden = testDriver.createInputTopic(OneShotRule.INPUT_TOPIC_OVERRIDDEN, OneShotRule.INPUT_KEY_OVERRIDDEN_SERDE.serializer(), OneShotRule.INPUT_VALUE_OVERRIDDEN_SERDE.serializer());
        inputTopicActive = testDriver.createInputTopic(OneShotRule.INPUT_TOPIC_ACTIVE, OneShotRule.INPUT_KEY_ACTIVE_SERDE.serializer(), OneShotRule.INPUT_VALUE_ACTIVE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(OneShotRule.OUTPUT_TOPIC, OneShotRule.OUTPUT_KEY_SERDE.deserializer(), OneShotRule.OUTPUT_VALUE_SERDE.deserializer());

        overriddenAlarmValue1 = new OverriddenAlarmValue();
        ShelvedAlarm shelvedAlarm1 = new ShelvedAlarm();
        shelvedAlarm1.setOneshot(true);
        shelvedAlarm1.setReason(ShelvedAlarmReason.Other);
        overriddenAlarmValue1.setMsg(shelvedAlarm1);

        overriddenAlarmValue2 = new OverriddenAlarmValue();
        ShelvedAlarm shelvedAlarm2 = new ShelvedAlarm();
        shelvedAlarm2.setOneshot(false);
        shelvedAlarm2.setReason(ShelvedAlarmReason.Other);
        overriddenAlarmValue2.setMsg(shelvedAlarm2);

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
    public void notOneshot() {
        inputTopicActive.pipeInput("alarm1", active1);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Shelved), overriddenAlarmValue2);
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void oneshot() {
        inputTopicActive.pipeInput("alarm1", null);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Shelved), overriddenAlarmValue1);
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());

        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = results.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertNull(result.value);
    }

    /*@Test
    public void transitionOverTime() {
        inputTopicActive.pipeInput("alarm1", active1);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Shelved), overriddenAlarmValue1);
        inputTopicActive.pipeInput("alarm1", null);
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());

        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = results.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertNull(result.value);
    }*/
}
