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
public class GroupByKTable {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Grouped grouped = Grouped.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        KTable<String, Stock> kTable = builder.table("stocks", consumed);
        KeyValueMapper<String, Stock, KeyValue<String, Stock>> keyValueMapper = (key, value) -> KeyValue.pair(String.format("%s-%s", "groupBy", key), value);
        KeyValueMapper<String, String, KeyValue<String, String>> keyValueMapperString = (key, value) -> KeyValue.pair(String.format("%s-%s", "groupBy", key), value);
        kTable.mapValues((key, value) -> value.getCompanyName()).groupBy(keyValueMapperString).reduce((aggValue, value) -> value, (aggValue, value) -> value).toStream().to("group-by");
        kTable.groupBy(keyValueMapper, grouped).reduce((aggValue, value) -> value, (aggValue, value) -> value).toStream().to("group-by-with-grouped");
        return builder.build();
    }
}
