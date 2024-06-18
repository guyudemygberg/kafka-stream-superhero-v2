package best.of.kafka.streams.aggrgate;

import best.of.kafka.streams.dto.Stock;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class GroupByKStream {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Consumed consumed = Consumed.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        Grouped grouped = Grouped.with(Serdes.String(), SerdesUtils.getSerde(Stock.class));
        KStream<String, Stock> kStream = builder.stream("stocks", consumed);
        KeyValueMapper<String, Stock, String> keyValueMapper = (key, value) -> String.format("%s-%s", "groupBy", key);
        kStream.groupByKey().reduce((aggValue, value) -> value).toStream().to("group-by-key");
        kStream.groupByKey(grouped).reduce((aggValue, value) -> value).toStream().to("group-by-key-with-grouped");
        kStream.groupBy(keyValueMapper).reduce((aggValue, value) -> value).toStream().to("group-by");
        kStream.groupBy(keyValueMapper, grouped).reduce((aggValue, value) -> value).toStream().to("group-by-with-grouped");
        return builder.build();
    }
}
