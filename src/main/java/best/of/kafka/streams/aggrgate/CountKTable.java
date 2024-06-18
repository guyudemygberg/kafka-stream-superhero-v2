package best.of.kafka.streams.aggrgate;

import best.of.kafka.streams.dto.Stock;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class CountKTable {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Materialized materialized = Materialized.as("CountKTableView");
        Materialized materializedWithName = Materialized.as("CountKTableNamedView");
        Grouped grouped = Grouped.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        KeyValueMapper<String, Stock, KeyValue<String, Stock>> keyValueMapper = (key, value) -> KeyValue.pair(String.format("%s-%s", "groupBy", key), value);
        KTable<String, Stock> kTable = builder.table("stocks", consumed);
        kTable.groupBy(keyValueMapper, grouped).count().toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "empty", key, value))).to("count");
        kTable.groupBy(keyValueMapper, grouped).count(Named.as("CountKTable")).toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "named", key, value))).to("count-with-name");
        kTable.groupBy(keyValueMapper, grouped).count(materialized).toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "view", key, value))).to("count-with-view");
        kTable.groupBy(keyValueMapper, grouped).count(Named.as("CountKTableWithName"), materializedWithName).toStream().peek((key, value) -> System.out.println(String.format(("%s-%s-%s"), "view-with-named", key, value))).to("count-with-name-with-view");
        return builder.build();
    }
}
