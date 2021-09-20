package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class MonologRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, RegisteredAlarm> inputTopicRegistered;
    private TestInputTopic<String, ActiveAlarm> inputTopicActive;
    private TestOutputTopic<String, MonologValue> outputTopic;
    private RegisteredAlarm registered1;
    private RegisteredAlarm registered2;
    private ActiveAlarm active1;
    private ActiveAlarm active2;

    @Before
    public void setup() {
        final MonologRule rule = new MonologRule();

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicRegistered = testDriver.createInputTopic(MonologRule.INPUT_TOPIC_REGISTERED, MonologRule.INPUT_KEY_REGISTERED_SERDE.serializer(), MonologRule.INPUT_VALUE_REGISTERED_SERDE.serializer());
        inputTopicActive = testDriver.createInputTopic(MonologRule.INPUT_TOPIC_ACTIVE, MonologRule.INPUT_KEY_ACTIVE_SERDE.serializer(), MonologRule.INPUT_VALUE_ACTIVE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(MonologRule.OUTPUT_TOPIC, MonologRule.MONOLOG_KEY_SERDE.deserializer(), MonologRule.MONOLOG_VALUE_SERDE.deserializer());

        registered1 = new RegisteredAlarm();
        registered2 = new RegisteredAlarm();

        registered1.setClass$("base");
        registered1.setProducer(new SimpleProducer());
        registered1.setLatching(true);

        registered2.setClass$("base");
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
    public void count() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegistered.pipeInput("alarm1", registered2);
        List<KeyValue<String, MonologValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void content() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegistered.pipeInput("alarm1", registered1);
        List<KeyValue<String, MonologValue>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());

        KeyValue<String, MonologValue> result = results.get(0);

        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals(new MonologValue(registered1, active1, new ArrayList<>()), result.value);
    }
}
