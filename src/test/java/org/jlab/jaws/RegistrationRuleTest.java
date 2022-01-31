package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class RegistrationRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, AlarmInstance> inputTopicRegistered;
    private TestInputTopic<String, AlarmClass> inputTopicClasses;
    private TestOutputTopic<String, EffectiveRegistration> outputTopicEffective;
    private TestOutputTopic<String, IntermediateMonolog> outputTopicMonolog;
    private AlarmInstance instance1;
    private AlarmInstance instance2;
    private AlarmClass class1;
    private AlarmInstance effectiveRegistered1;

    @Before
    public void setup() {
        final RegistrationRule rule = new RegistrationRule("alarm-classes", "alarm-instances", "effective-registrations", "intermediate-registration");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicClasses = testDriver.createInputTopic(rule.inputTopicClasses, RegistrationRule.INPUT_KEY_CLASSES_SERDE.serializer(), RegistrationRule.INPUT_VALUE_CLASSES_SERDE.serializer());
        inputTopicRegistered = testDriver.createInputTopic(rule.inputTopicInstances, RegistrationRule.INPUT_KEY_INSTANCES_SERDE.serializer(), RegistrationRule.INPUT_VALUE_INSTANCES_SERDE.serializer());
        outputTopicEffective = testDriver.createOutputTopic(rule.outputTopicEffective, RegistrationRule.EFFECTIVE_KEY_SERDE.deserializer(), RegistrationRule.EFFECTIVE_VALUE_SERDE.deserializer());
        outputTopicMonolog = testDriver.createOutputTopic(rule.outputTopicMonolog, RegistrationRule.MONOLOG_KEY_SERDE.deserializer(), RegistrationRule.MONOLOG_VALUE_SERDE.deserializer());


        instance1 = new AlarmInstance();
        instance2 = new AlarmInstance();

        instance1.setClass$("base");
        instance1.setProducer(new SimpleProducer());
        instance1.setLocation(Arrays.asList("NL"));

        instance2.setClass$("base");
        instance2.setProducer(new SimpleProducer());
        instance2.setLocation(Arrays.asList("NL"));

        class1 = new AlarmClass();
        class1.setLatching(true);
        class1.setCategory("CAMAC");
        class1.setFilterable(true);
        class1.setCorrectiveaction("fix it");
        class1.setPriority(AlarmPriority.P3_MINOR);
        class1.setPointofcontactusername("tester");
        class1.setRationale("because");

        class1.setOffdelayseconds(5l);
        class1.setOndelayseconds(5l);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void count() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", instance2);
        List<KeyValue<String, EffectiveRegistration>> results = outputTopicEffective.readKeyValuesToList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void content() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", instance1);
        List<KeyValue<String, EffectiveRegistration>> results = outputTopicEffective.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, EffectiveRegistration> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(1, results.size());

        KeyValue<String, EffectiveRegistration> result1 = results.get(0);

        Assert.assertEquals("alarm1", result1.key);
    }

    @Test
    public void noClass() {
        inputTopicRegistered.pipeInput("alarm1", instance1);
        List<KeyValue<String, EffectiveRegistration>> results = outputTopicEffective.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, EffectiveRegistration> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(1, results.size());

        KeyValue<String, EffectiveRegistration> result1 = results.get(0);

        Assert.assertEquals("alarm1", result1.key);
    }

    @Test
    public void tomestoneRegistration() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", instance1);
        inputTopicRegistered.pipeInput("alarm1", null);
        List<KeyValue<String, EffectiveRegistration>> results = outputTopicEffective.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, EffectiveRegistration> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(2, results.size());

        KeyValue<String, EffectiveRegistration> result2 = results.get(1);

        Assert.assertEquals("alarm1", result2.key);
        Assert.assertNull(result2.value);
    }
}
