package best.of.kafka.streams.aggrgate;

import best.of.kafka.streams.dto.Stock;
import best.of.kafka.streams.dto.StockAgg;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class AggregateKTable {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Materialized materialized = Materialized.as("AggregateKTableView").with(Serdes.String(), SerdesUtils.getSerde(StockAgg.class));
        Materialized materializedWithName = Materialized.as("AggregateKTableNamedView").with(Serdes.String(), SerdesUtils.getSerde(StockAgg.class));
        KTable<String, Stock> kTable = builder.table("stocks", consumed);
        Grouped<String, Stock> grouped = Grouped.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        KeyValueMapper<String, Stock, KeyValue<String, Stock>> keyValueMapper = (key, value) -> KeyValue.pair("S&P500", value);

//        kTable.groupBy(keyValueMapper, grouped).aggregate(StockAgg::new, StockAgg::addToTotal, StockAgg::subtractFromTotal).toStream().to("aggregate");
        kTable.groupBy(keyValueMapper, grouped).aggregate(StockAgg::new, StockAgg::addToTotal, StockAgg::subtractFromTotal, materialized).toStream().to("aggregate-with-view");
        kTable.groupBy(keyValueMapper, grouped).aggregate(StockAgg::new, StockAgg::addToTotal, StockAgg::subtractFromTotal, Named.as("AggregateKTable"), materializedWithName).toStream().to("aggregate-with-view-with-name");

        return builder.build();
    }
}
