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

public class EffectiveRegistrationRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, AlarmRegistration> inputTopicRegistered;
    private TestInputTopic<String, AlarmClass> inputTopicClasses;
    private TestOutputTopic<String, AlarmRegistration> outputTopic;
    private AlarmRegistration registered1;
    private AlarmRegistration registered2;
    private AlarmClass class1;
    private AlarmRegistration effectiveRegistered1;

    @Before
    public void setup() {
        final EffectiveRegistrationRule rule = new EffectiveRegistrationRule("registered-classes", "registered-alarms", "effective-registrations");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicClasses = testDriver.createInputTopic(rule.inputTopicClasses, EffectiveRegistrationRule.INPUT_KEY_CLASSES_SERDE.serializer(), EffectiveRegistrationRule.INPUT_VALUE_CLASSES_SERDE.serializer());
        inputTopicRegistered = testDriver.createInputTopic(rule.inputTopicRegistered, EffectiveRegistrationRule.INPUT_KEY_REGISTERED_SERDE.serializer(), EffectiveRegistrationRule.INPUT_VALUE_REGISTERED_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(rule.outputTopic, EffectiveRegistrationRule.EFFECTIVE_KEY_SERDE.deserializer(), EffectiveRegistrationRule.EFFECTIVE_VALUE_SERDE.deserializer());

        registered1 = new AlarmRegistration();
        registered2 = new AlarmRegistration();

        registered1.setClass$("base");
        registered1.setProducer(new SimpleProducer());
        registered1.setLatching(true);

        registered2.setClass$("base");
        registered2.setProducer(new SimpleProducer());
        registered2.setLatching(false);

        class1 = new AlarmClass();
        class1.setLatching(true);
        class1.setCategory(AlarmCategory.CAMAC);
        class1.setFilterable(true);
        class1.setCorrectiveaction("fix it");
        class1.setLocation(AlarmLocation.A4);
        class1.setPriority(AlarmPriority.P3_MINOR);
        class1.setScreenpath("/tmp");
        class1.setPointofcontactusername("tester");
        class1.setRationale("because");

        class1.setMaskedby("alarm1");
        class1.setOffdelayseconds(5l);
        class1.setOndelayseconds(5l);

        effectiveRegistered1 = EffectiveRegistrationRule.computeEffectiveRegistration(registered1, class1);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void count() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", registered2);
        List<KeyValue<String, AlarmRegistration>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void content() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", registered1);
        List<KeyValue<String, AlarmRegistration>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, AlarmRegistration> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(1, results.size());

        KeyValue<String, AlarmRegistration> result1 = results.get(0);

        AlarmRegistration expectedRegistration = EffectiveRegistrationRule.computeEffectiveRegistration(registered1, class1);

        Assert.assertEquals("alarm1", result1.key);
        Assert.assertEquals(expectedRegistration, result1.value);
    }
}
