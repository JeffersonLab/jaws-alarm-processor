package org.jlab.jaws;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.jlab.jaws.entity.OverriddenAlarmKey;
import org.jlab.jaws.entity.OverriddenAlarmType;
import org.jlab.jaws.entity.OverriddenAlarmValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class LatchRule extends AutoOverrideRule {
    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-auto-override-processor-latch");

        return props;
    }

    @Override
    public Topology constructTopology(Properties props) {
        return null;
    }
}
