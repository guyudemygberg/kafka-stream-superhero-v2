package best.of.kafka.streams.kstreamktablemethods;

import best.of.kafka.streams.dto.DetailedJob;
import best.of.kafka.streams.dto.Job;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class FlatMapAndFlatMapValues {

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sentenceKStream = builder.stream("sentence", Consumed.with(Serdes.String(), Serdes.String()));

        KeyValueMapper<String, String, List<KeyValue<String, String>>> keyValueMapper = (key, sentence) -> Arrays.stream(sentence.split(" "))
                                                                                                              .map(word -> KeyValue.pair(key, word)).collect(Collectors.toList());;
        ValueMapper<String, List<String>> valueMapper = (sentence) -> Arrays.asList(sentence.split(" "));
        ValueMapperWithKey<String, String, List<String>> valueMapperWithKey = (key, sentence) -> Arrays.stream(sentence.split(" "))
                                                                                                    .map(word -> key + "-" + word).collect(Collectors.toList());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        KStream<String, String> flatMapKStream = sentenceKStream.flatMap(keyValueMapper);
        KStream<String, String> flatMapKStreamWithNamed = sentenceKStream.flatMap(keyValueMapper, Named.as("flatMap-stream"));
        KStream<String, String> flatMapValuesKStream = sentenceKStream.flatMapValues(valueMapper);
        KStream<String, String> flatMapValuesKStreamWithNamed = sentenceKStream.flatMapValues(valueMapper, Named.as("flatMap-with-values-stream"));
        KStream<String, String> flatMapValuesKStreamWithKey = sentenceKStream.flatMapValues(valueMapperWithKey);
        KStream<String, String> flatMapValuesKStreamWithNamedWithKey = sentenceKStream.flatMapValues(valueMapperWithKey, Named.as("flatMap-with-values-stream-with-key"));

        flatMapKStream.to("flatMapKStream", produced);
        flatMapKStreamWithNamed.to("flatMapKStreamWithNamed", produced);
        flatMapValuesKStream.to("flatMapValuesKStream", produced);
        flatMapValuesKStreamWithNamed.to("flatMapValuesKStreamWithNamed", produced);
        flatMapValuesKStreamWithKey.to("flatMapValuesKStreamWithKey", produced);
        flatMapValuesKStreamWithNamedWithKey.to("flatMapValuesKStreamWithNamedWithKey", produced);

        return builder.build();
    }
}
