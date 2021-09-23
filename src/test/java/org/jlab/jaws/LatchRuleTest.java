package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.MonitorInfo;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class LatchRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MonologValue> inputTopicMonolog;
    private TestOutputTopic<OverriddenAlarmKey, OverriddenAlarmValue> outputTopic;
    private RegisteredAlarm registered1;
    private RegisteredAlarm registered2;
    private RegisteredClass class1;
    private ActiveAlarm active1;
    private ActiveAlarm active2;

    @Before
    public void setup() {
        final LatchRule rule = new LatchRule();

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);

        System.err.println(top.describe());

        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicMonolog = testDriver.createInputTopic(LatchRule.INPUT_TOPIC, LatchRule.MONOLOG_KEY_SERDE.serializer(), LatchRule.MONOLOG_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(LatchRule.OUTPUT_TOPIC_PASSTHROUGH, LatchRule.OVERRIDE_KEY_SERDE.deserializer(), LatchRule.OVERRIDE_VALUE_SERDE.deserializer());

        registered1 = new RegisteredAlarm();
        registered2 = new RegisteredAlarm();

        registered1.setClass$("base");
        registered1.setProducer(new SimpleProducer());
        registered1.setLatching(true);

        registered2.setClass$("base");
        registered2.setProducer(new SimpleProducer());
        registered2.setLatching(false);

        class1 = new RegisteredClass();
        class1.setLatching(true);
        class1.setCategory(AlarmCategory.CAMAC);
        class1.setFilterable(true);
        class1.setCorrectiveaction("fix it");
        class1.setLocation(AlarmLocation.A4);
        class1.setPriority(AlarmPriority.P3_MINOR);
        class1.setScreenpath("/tmp");

        active1 = new ActiveAlarm();
        active2 = new ActiveAlarm();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    //@Test
    public void notLatching() {
        inputTopicMonolog.pipeInput("alarm1", new MonologValue(registered2, class1, null, active1, new ArrayList<>(), false, false));
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(0, results.size());
    }

    //@Test
    public void latching() {
        inputTopicMonolog.pipeInput("alarm1", new MonologValue(registered1, class1, null, active1, new ArrayList<>(), false, false));
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());

        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = results.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertEquals(new OverriddenAlarmValue(new LatchedAlarm()), result.value);
    }
}
