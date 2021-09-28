package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class OneShotRuleTest {
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
        final OneShotRule rule = new OneShotRule();

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicMonolog = testDriver.createInputTopic(OneShotRule.INPUT_TOPIC, OneShotRule.MONOLOG_KEY_SERDE.serializer(), OneShotRule.MONOLOG_VALUE_SERDE.serializer());
        outputPassthroughTopic = testDriver.createOutputTopic(OneShotRule.OUTPUT_TOPIC_PASSTHROUGH, OneShotRule.MONOLOG_KEY_SERDE.deserializer(), OneShotRule.MONOLOG_VALUE_SERDE.deserializer());
        outputOverrideTopic = testDriver.createOutputTopic(OneShotRule.OUTPUT_TOPIC_OVERRIDE, OneShotRule.OVERRIDE_KEY_SERDE.deserializer(), OneShotRule.OVERRIDE_VALUE_SERDE.deserializer());

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
    public void notOneshot() {
        ShelvedAlarm shelved = new ShelvedAlarm();
        shelved.setOneshot(false);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedAlarmReason.Other);
        mono1.getOverrides().setShelved(shelved);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, MonologValue>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(0, overrideResults.size());
        Assert.assertEquals(1, passthroughResults.size());
    }

    @Test
    public void oneshot() {
        ShelvedAlarm shelved = new ShelvedAlarm();
        shelved.setOneshot(true);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedAlarmReason.Other);
        mono1.getOverrides().setShelved(shelved);

        mono1.getTransitions().setTransitionToActive(false);
        mono1.getTransitions().setTransitionToNormal(true);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, MonologValue>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(1, overrideResults.size());
        Assert.assertEquals(0, passthroughResults.size());
    }

    @Test
    public void oneshotABunch() {
        ShelvedAlarm shelved = new ShelvedAlarm();
        shelved.setOneshot(true);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedAlarmReason.Other);
        mono1.getOverrides().setShelved(shelved);

        inputTopicMonolog.pipeInput("alarm1", mono1);


        MonologValue mono2 = MonologValue.newBuilder(mono1).build();

        inputTopicMonolog.pipeInput("alarm1", mono2);



        List<KeyValue<String, MonologValue>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(0, overrideResults.size());
        Assert.assertEquals(2, passthroughResults.size());
    }
}
