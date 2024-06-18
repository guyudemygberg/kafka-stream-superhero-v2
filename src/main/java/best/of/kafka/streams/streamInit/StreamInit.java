package best.of.kafka.streams.streamInit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Properties;

@Component
public class StreamInit {

    private final KafkaStreams streams;

    public StreamInit(Topology topology){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"app-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        this.streams = new KafkaStreams(topology, props);

    }

    @PostConstruct
    public void startStream(){
        this.streams.cleanUp();
        this.streams.start();
    }

    @PreDestroy
    public void closeStream(){
        this.streams.close();
    }
}
