package best.of.kafka.streams.aggrgate;

import best.of.kafka.streams.dto.Stock;
import best.of.kafka.streams.dto.StockAgg;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class AggregateKStream {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Materialized materialized = Materialized.as("AggregateKStreamView");//.with(Serdes.String(), SerdesUtils.getSerde(StockAgg.class));
        Materialized materializedWithName = Materialized.as("AggregateKStreamNamedView");//.with(Serdes.String(), SerdesUtils.getSerde(StockAgg.class));
        KStream<String, Stock> kStream = builder.stream("stocks", consumed);

        kStream.groupByKey().aggregate(StockAgg::new, StockAgg::calcAvg).toStream().to("aggregate");
        kStream.groupByKey().aggregate(StockAgg::new, StockAgg::calcAvg, materialized).toStream().to("aggregate-with-view");
        kStream.groupByKey().aggregate(StockAgg::new, StockAgg::calcAvg, Named.as("AggregateKStream"), materializedWithName).toStream().to("aggregate-with-view-with-name");

        return builder.build();
    }
}
