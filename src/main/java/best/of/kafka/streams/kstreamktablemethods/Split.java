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
public class Split {

    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Alcohol> beerKStream = builder.stream("beer", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class)));
        KStream<String, Alcohol> vodkaKStream = builder.stream("vodka", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class)));
        KStream<String, Alcohol> whiskeyKStream = builder.stream("whiskey", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class)));

        Predicate<String, Alcohol> isUsaAlcohol = (key, value) -> value.getCountry().equals("USA");
        Predicate<String, Alcohol> isIrelandAlcohol = (key, value) -> value.getCountry().equals("Ireland");
        Produced<String, Alcohol> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(Alcohol.class));
        Branched<String, Alcohol> usaBranched = Branched.withConsumer(ks -> ks.to("usa-alcohol", produced));
        Branched<String, Alcohol> irelandBranched = Branched.withConsumer(ks -> ks.to("ireland-alcohol", produced));
        Branched<String, Alcohol> defaultBranched = Branched.withConsumer(ks -> ks.to("unknown-alcohol", produced));

        beerKStream.merge(vodkaKStream).merge(whiskeyKStream).split()
                .branch(isUsaAlcohol, usaBranched)
                .branch(isIrelandAlcohol, irelandBranched)
                .defaultBranch(defaultBranched);

        return builder.build();
    }
}
