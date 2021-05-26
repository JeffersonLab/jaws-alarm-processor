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

public class AutoOverrideProcessorTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<OverriddenAlarmKey, OverriddenAlarmValue> inputTopic;
    private TestOutputTopic<OverriddenAlarmKey, OverriddenAlarmValue> outputTopic;
    private ShelvedAlarm alarm1;
    private ShelvedAlarm alarm2;

    @Before
    public void setup() {
        final Properties streamsConfig = AutoOverrideProcessor.getStreamsConfig();
        streamsConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = AutoOverrideProcessor.createTopology(streamsConfig);
        testDriver = new TopologyTestDriver(top, streamsConfig);

        // setup test topics
        inputTopic = testDriver.createInputTopic(AutoOverrideProcessor.INPUT_TOPIC, AutoOverrideProcessor.INPUT_KEY_SERDE.serializer(), AutoOverrideProcessor.INPUT_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(AutoOverrideProcessor.OUTPUT_TOPIC, AutoOverrideProcessor.OUTPUT_KEY_SERDE.deserializer(), AutoOverrideProcessor.OUTPUT_VALUE_SERDE.deserializer());

        alarm1 = new ShelvedAlarm();
        alarm1.setReason(ShelvedAlarmReason.Chattering_Fleeting_Alarm);
        alarm1.setExpiration(Instant.now().plusSeconds(5).getEpochSecond() * 1000);

        alarm2 = new ShelvedAlarm();
        alarm2.setReason(ShelvedAlarmReason.Chattering_Fleeting_Alarm);
        alarm2.setExpiration(Instant.now().plusSeconds(5).getEpochSecond() * 1000);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void tombstoneMsg() throws InterruptedException {
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> keyValues = new ArrayList<>();
        keyValues.add(KeyValue.pair(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Shelved), new OverriddenAlarmValue(alarm1)));
        keyValues.add(KeyValue.pair(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Shelved), new OverriddenAlarmValue(alarm2)));
        inputTopic.pipeKeyValueList(keyValues, Instant.now(), Duration.ofSeconds(5));
        testDriver.advanceWallClockTime(Duration.ofSeconds(5));
        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = outputTopic.readKeyValuesToList().get(0);
        Assert.assertNull(result.value);
    }

    @Test
    public void notYetExpired() {
        inputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Shelved), new OverriddenAlarmValue(alarm1));
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopic.pipeInput(new OverriddenAlarmKey("alarm2", OverriddenAlarmType.Shelved), new OverriddenAlarmValue(alarm2));
        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = outputTopic.readKeyValuesToList().get(0);
        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertNull(result.value);
    }

    @Test
    public void expired() {
        inputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Shelved), new OverriddenAlarmValue(alarm1));
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = outputTopic.readKeyValue();
        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertNull(result.value);
    }
}
