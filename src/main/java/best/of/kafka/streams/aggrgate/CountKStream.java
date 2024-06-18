package best.of.kafka.streams.aggrgate;

import best.of.kafka.streams.dto.Stock;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class CountKStream {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Materialized materialized = Materialized.as("CountKStreamView");
        Materialized materializedWithName = Materialized.as("CountKStreamNamedView");
        KStream<String, Stock> kStream = builder.stream("stocks", consumed);
        kStream.groupByKey().count().toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "empty", key, value))).to("count");
        kStream.groupByKey().count(Named.as("CountKStream")).toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "named", key, value))).to("count-with-name");
        kStream.groupByKey().count(materialized).toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "view", key, value))).to("count-with-view");
        kStream.groupByKey().count(Named.as("CountKStreamWithName"), materializedWithName).toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "view-with-named", key, value))).to("count-with-name-with-view");
        return builder.build();
    }
}
