package best.of.kafka.streams.kstreamktablemethods;

import best.of.kafka.streams.dto.Alcohol;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class Merge {

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Alcohol> beerKStream = builder.stream("beer", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class)));
        KStream<String, Alcohol> vodkaKStream = builder.stream("vodka", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class)));
        KStream<String, Alcohol> whiskeyKStream = builder.stream("whiskey", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class)));

        Produced<String, Alcohol> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class));

        beerKStream.merge(vodkaKStream).merge(whiskeyKStream).to("party-booze", produced);
        beerKStream.merge(vodkaKStream, Named.as("beer-vodka")).merge(whiskeyKStream, Named.as("beer-vodka-whiskey")).to("party-booze-with-name", produced);

        return builder.build();
    }
}
