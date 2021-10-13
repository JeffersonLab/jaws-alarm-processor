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

public class EffectiveStateRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Alarm> inputTopic;
    private TestOutputTopic<String, Alarm> outputTopic;
    private AlarmRegistration registered1;
    private AlarmRegistration registered2;
    private AlarmClass class1;
    private AlarmActivationUnion active1;
    private AlarmActivationUnion active2;
    private Alarm mono1;

    @Before
    public void setup() {
        final EffectiveStateRule rule = new EffectiveStateRule("monolog", "alarms");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);

        //System.err.println(top.describe());

        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic(rule.inputTopic, EffectiveStateRule.MONOLOG_KEY_SERDE.serializer(), EffectiveStateRule.MONOLOG_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(rule.outputTopic, EffectiveStateRule.MONOLOG_KEY_SERDE.deserializer(), EffectiveStateRule.MONOLOG_VALUE_SERDE.deserializer());
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

        active1 = new AlarmActivationUnion();
        active2 = new AlarmActivationUnion();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());

        mono1 = new Alarm();
        mono1.setActivation(active1);
        mono1.setClass$(class1);
        mono1.setRegistration(registered1);
        mono1.setEffectiveRegistration(MonologRule.computeEffectiveRegistration(registered1, class1));
        mono1.setOverrides(new AlarmOverrideSet());
        mono1.setTransitions(new ProcessorTransitions());
        mono1.getTransitions().setTransitionToActive(true);
        mono1.getTransitions().setTransitionToNormal(false);
        mono1.setState(AlarmState.Normal);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void notLatching() {
        mono1.setActivation(null);

        inputTopic.pipeInput("alarm1", mono1);
        List<KeyValue<String, Alarm>> stateResults = outputTopic.readKeyValuesToList();

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("Normal", stateResults.get(0).value.getState().name());
    }

    @Test
    public void latching() {
        mono1.getEffectiveRegistration().setLatching(true);
        mono1.setActivation(null);
        mono1.getTransitions().setLatching(true);

        inputTopic.pipeInput("alarm1", mono1);
        List<KeyValue<String, Alarm>> stateResults = outputTopic.readKeyValuesToList();

        System.err.println("\n\nInitial State:");
        for(KeyValue<String, Alarm> pass: stateResults) {
            System.err.println(pass);
        }

        System.err.println("\n");

        Assert.assertEquals(1, stateResults.size());

        KeyValue<String, Alarm> passResult = stateResults.get(0);

        Assert.assertEquals("ActiveLatched", passResult.value.getState().name());

        Alarm mono2 = Alarm.newBuilder(mono1).build();

        mono2.getOverrides().setLatched(new LatchedOverride());

        inputTopic.pipeInput("alarm1", mono2);

        stateResults = outputTopic.readKeyValuesToList();

        System.err.println("\n\nFinal State:");
        for(KeyValue<String, Alarm> pass: stateResults) {
            System.err.println(pass);
        }

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("ActiveLatched", passResult.value.getState().name());
    }
}
