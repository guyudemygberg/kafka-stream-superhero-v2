package best.of.kafka.streams.kstreamktablemethods;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class SelectKey {

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sentenceKStream = builder.stream("sentence", Consumed.with(Serdes.String(), Serdes.String()));

        KeyValueMapper<String, String, String> keyValueMapper = (key, sentence) -> "interesting " + key;
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        KStream<String, String> selectKeyKStream = sentenceKStream.selectKey(keyValueMapper);
        KStream<String, String> selectKeyKStreamWithNamed = sentenceKStream.selectKey(keyValueMapper, Named.as("selectKey-stream"));

        selectKeyKStream.to("selectKeyKStream", produced);
        selectKeyKStreamWithNamed.to("selectKeyKStreamWithNamed", produced);

        return builder.build();
    }
}
