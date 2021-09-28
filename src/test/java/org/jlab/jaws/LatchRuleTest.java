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
    private TestOutputTopic<String, MonologValue> outputPassthroughTopic;
    private TestOutputTopic<OverriddenAlarmKey, OverriddenAlarmValue> outputOverrideTopic;
    private RegisteredAlarm registered1;
    private RegisteredAlarm registered2;
    private RegisteredClass class1;
    private ActiveAlarm active1;
    private ActiveAlarm active2;
    private MonologValue mono1;

    @Before
    public void setup() {
        final LatchRule rule = new LatchRule();

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);

        //System.err.println(top.describe());

        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicMonolog = testDriver.createInputTopic(LatchRule.INPUT_TOPIC, LatchRule.MONOLOG_KEY_SERDE.serializer(), LatchRule.MONOLOG_VALUE_SERDE.serializer());
        outputPassthroughTopic = testDriver.createOutputTopic(LatchRule.OUTPUT_TOPIC_PASSTHROUGH, LatchRule.MONOLOG_KEY_SERDE.deserializer(), LatchRule.MONOLOG_VALUE_SERDE.deserializer());
        outputOverrideTopic = testDriver.createOutputTopic(LatchRule.OUTPUT_TOPIC_OVERRIDE, LatchRule.OVERRIDE_KEY_SERDE.deserializer(), LatchRule.OVERRIDE_VALUE_SERDE.deserializer());

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
        class1.setPointofcontactusername("tester");
        class1.setRationale("because");

        active1 = new ActiveAlarm();
        active2 = new ActiveAlarm();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());

        mono1 = new MonologValue();
        mono1.setActive(active1);
        mono1.setClass$(class1);
        mono1.setRegistered(registered1);
        mono1.setEffectiveRegistered(MonologRule.computeEffectiveRegistration(registered1, class1));
        mono1.setOverrides(new OverrideSet());
        mono1.setTransitions(new TransitionSet());
        mono1.getTransitions().setTransitionToActive(true);
        mono1.getTransitions().setTransitionToNormal(false);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void notLatching() {
        mono1.getEffectiveRegistered().setLatching(false);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        List<KeyValue<String, MonologValue>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(0, overrideResults.size());
    }

    @Test
    public void latching() {
        mono1.getEffectiveRegistered().setLatching(true);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, MonologValue>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        System.err.println("\n\nInitial Passthrough:");
        for(KeyValue<String, MonologValue> pass: passthroughResults) {
            System.err.println(pass);
        }

        System.err.println("\n\nInitial Overrides:");
        for(KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> over: overrideResults) {
            System.err.println(over);
        }

        System.err.println("\n");

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(1, overrideResults.size());

        KeyValue<String, MonologValue> passResult = passthroughResults.get(0);

        Assert.assertEquals(true, passResult.value.getTransitions().getLatching());

        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = overrideResults.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertEquals(new OverriddenAlarmValue(new LatchedAlarm()), result.value);

        MonologValue mono2 = MonologValue.newBuilder(mono1).build();

        mono2.getOverrides().setLatched(new LatchedAlarm());
        mono2.getTransitions().setTransitionToActive(false);

        inputTopicMonolog.pipeInput("alarm1", mono2);

        passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        overrideResults = outputOverrideTopic.readKeyValuesToList();

        System.err.println("\n\nFinal Passthrough:");
        for(KeyValue<String, MonologValue> pass: passthroughResults) {
            System.err.println(pass);
        }

        System.err.println("\n\nFinal Overrides:");
        for(KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> over: overrideResults) {
            System.err.println(over);
        }

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(0, overrideResults.size());
    }
}
