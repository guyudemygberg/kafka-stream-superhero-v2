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
public class ReduceKStream {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Materialized materialized = Materialized.as("ReduceKStreamView");
        Materialized materializedWithName = Materialized.as("ReduceStreamNamedView");
        KStream<String, Stock> kStream = builder.stream("stocks", consumed);
        Reducer<Stock> reducer = (aggValue, currentValue) -> {
            System.out.println(String.format("AggValue - Company: %s | Price: %.2f | Hits: %d", aggValue.getCompanyName(), aggValue.getPrice(), aggValue.getNumberOfHits()) );
            System.out.println(String.format("CurrentValue - Company: %s | Price: %.2f | Hits: %d", currentValue.getCompanyName(), currentValue.getPrice(), currentValue.getNumberOfHits()) );
            System.out.println("-----------------------------------------");
            return aggValue.addPrice(currentValue);
        };
        kStream.groupByKey().reduce(reducer).toStream().to("reduce");
        kStream.groupByKey().reduce(reducer, materialized).toStream().to("reduce-with-view");
        kStream.groupByKey().reduce(reducer, Named.as("ReduceKStream"), materializedWithName).toStream().to("reduce-with-view-with-name");

        return builder.build();
    }
}
