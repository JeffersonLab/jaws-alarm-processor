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

public class RegistrationRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, AlarmRegistration> inputTopicRegistered;
    private TestInputTopic<String, AlarmClass> inputTopicClasses;
    private TestOutputTopic<String, EffectiveRegistration> outputTopicEffective;
    private TestOutputTopic<String, IntermediateMonolog> outputTopicMonolog;
    private AlarmRegistration registered1;
    private AlarmRegistration registered2;
    private AlarmClass class1;
    private AlarmRegistration effectiveRegistered1;

    @Before
    public void setup() {
        final RegistrationRule rule = new RegistrationRule("alarm-classes", "alarm-registrations", "effective-registrations", "intermediate-registration");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicClasses = testDriver.createInputTopic(rule.inputTopicClasses, RegistrationRule.INPUT_KEY_CLASSES_SERDE.serializer(), RegistrationRule.INPUT_VALUE_CLASSES_SERDE.serializer());
        inputTopicRegistered = testDriver.createInputTopic(rule.inputTopicRegistered, RegistrationRule.INPUT_KEY_REGISTERED_SERDE.serializer(), RegistrationRule.INPUT_VALUE_REGISTERED_SERDE.serializer());
        outputTopicEffective = testDriver.createOutputTopic(rule.outputTopicEffective, RegistrationRule.EFFECTIVE_KEY_SERDE.deserializer(), RegistrationRule.EFFECTIVE_VALUE_SERDE.deserializer());
        outputTopicMonolog = testDriver.createOutputTopic(rule.outputTopicMonolog, RegistrationRule.MONOLOG_KEY_SERDE.deserializer(), RegistrationRule.MONOLOG_VALUE_SERDE.deserializer());


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

        effectiveRegistered1 = RegistrationRule.computeEffectiveRegistration(registered1, class1);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void count() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", registered2);
        List<KeyValue<String, EffectiveRegistration>> results = outputTopicEffective.readKeyValuesToList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void content() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", registered1);
        List<KeyValue<String, EffectiveRegistration>> results = outputTopicEffective.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, EffectiveRegistration> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(1, results.size());

        KeyValue<String, EffectiveRegistration> result1 = results.get(0);

        AlarmRegistration expectedRegistration = RegistrationRule.computeEffectiveRegistration(registered1, class1);

        Assert.assertEquals("alarm1", result1.key);
        Assert.assertEquals(expectedRegistration, result1.value.getCalculated());
    }

    @Test
    public void noClass() {
        inputTopicRegistered.pipeInput("alarm1", registered1);
        List<KeyValue<String, EffectiveRegistration>> results = outputTopicEffective.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, EffectiveRegistration> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(1, results.size());

        KeyValue<String, EffectiveRegistration> result1 = results.get(0);

        AlarmRegistration expectedRegistration = registered1;

        Assert.assertEquals("alarm1", result1.key);
        Assert.assertEquals(expectedRegistration, result1.value.getCalculated());
    }
}
