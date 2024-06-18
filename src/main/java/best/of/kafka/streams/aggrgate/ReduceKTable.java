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
public class ReduceKTable {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Materialized materialized = Materialized.as("ReduceKStreamView");
        Materialized materializedWithName = Materialized.as("ReduceKStreamNamedView");
        KTable<String, Stock> kTable = builder.table("stocks", consumed);
        Grouped grouped = Grouped.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        KeyValueMapper<String, Stock, KeyValue<String, Stock>> keyValueMapper = (key, value) -> KeyValue.pair("S&P500", value);
        Reducer<Stock> adder = (aggValue, currentValue) -> {
           Double currentTotalIncome = aggValue.getTotalIncome() == null ? aggValue.getPrice() : aggValue.getTotalIncome();
           aggValue.setTotalIncome(currentTotalIncome + currentValue.getPrice());
           return aggValue;
        };
        Reducer<Stock> subtractor = (aggValue, currentValue) -> {
            aggValue.setTotalIncome(aggValue.getTotalIncome() - currentValue.getPrice());
            return aggValue;
        };
        kTable.groupBy(keyValueMapper, grouped).reduce(adder, subtractor).toStream().to("reduce");
        kTable.groupBy(keyValueMapper, grouped).reduce(adder, subtractor, materialized).toStream().to("reduce-with-view");
        kTable.groupBy(keyValueMapper, grouped).reduce(adder, subtractor, Named.as("ReduceKStream"), materializedWithName).toStream().to("reduce-with-view-with-name");

        return builder.build();
    }
}
