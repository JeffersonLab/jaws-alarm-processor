package org.jlab.kafka.streams;

import org.apache.kafka.streams.*;
import org.jlab.kafka.alarms.ShelvedAlarm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class ShelvedTimerTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ShelvedAlarm> inputTopic;
    private TestOutputTopic<String, ShelvedAlarm> outputTopic;
    private ShelvedAlarm alarm1;

    @Before
    public void setup() {
        final Properties streamsConfig = ShelvedTimer.getStreamsConfig();
        streamsConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = ShelvedTimer.createTopology(streamsConfig);
        testDriver = new TopologyTestDriver(top, streamsConfig);

        // setup test topics
        inputTopic = testDriver.createInputTopic(ShelvedTimer.INPUT_TOPIC, ShelvedTimer.INPUT_KEY_SERDE.serializer(), ShelvedTimer.INPUT_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(ShelvedTimer.OUTPUT_TOPIC, ShelvedTimer.OUTPUT_KEY_SERDE.deserializer(), ShelvedTimer.OUTPUT_VALUE_SERDE.deserializer());

        alarm1 = new ShelvedAlarm();
        alarm1.setReason("Testing");
        alarm1.setExpiration(Instant.now().plusSeconds(5).getEpochSecond() * 1000);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void matchedTombstoneMsg() {
        inputTopic.pipeInput("alarm1", alarm1);
        inputTopic.pipeInput("alarm1", null);
        KeyValue<String, ShelvedAlarm> result = outputTopic.readKeyValuesToList().get(1);
        Assert.assertNull(result.value);
    }


    @Test
    public void unmatchedTombstoneMsg() {
        inputTopic.pipeInput("alarm1", null);
        Assert.assertTrue(outputTopic.isEmpty()); // Cannot transform a tombstone without a prior registration!
    }

    @Test
    public void regularMsg() {
        inputTopic.pipeInput("alarm1", alarm1);
        KeyValue<String, ShelvedAlarm> result = outputTopic.readKeyValue();
        Assert.assertEquals("{\"topic\":\"active-alarms\",\"channel\":\"channel1\"}", result.key);
        Assert.assertEquals("{\"mask\":\"a\",\"outkey\":\"alarm1\"}", result.value);
    }
}
