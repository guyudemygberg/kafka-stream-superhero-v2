package best.of.kafka.streams.kstreamktablemethods;

import best.of.kafka.streams.dto.DetailedJob;
import best.of.kafka.streams.dto.Job;
import best.of.kafka.streams.dto.People;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class MapAndMapValues {

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Job> jobKStream = builder.stream("JobKStream", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Job.class)));
        KTable<String, Job> jobKTable = builder.table("JobKTable", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Job.class)));

        KeyValueMapper<String, Job, KeyValue<String, DetailedJob>> keyValueMapper = (key, value) -> KeyValue.pair(value.getCompanyName(), new DetailedJob(value));
        ValueMapper<Job, DetailedJob> valueMapper = (value) -> new DetailedJob(value);
        ValueMapperWithKey<String, Job, DetailedJob> valueMapperWithKey = (key, value) -> new DetailedJob(key, value);
        Materialized<String, DetailedJob, KeyValueStore<Bytes, byte[]>> detailedJobView = Materialized.as("detailed-job-view").with(Serdes.String(), SerdesUtils.getSerde(DetailedJob.class));
        Produced<String, DetailedJob> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(DetailedJob.class));

        KStream<String, DetailedJob> mapKStream = jobKStream.map(keyValueMapper);
        KStream<String, DetailedJob> mapKStreamWithNamed = jobKStream.map(keyValueMapper, Named.as("map-stream"));
        KStream<String, DetailedJob> mapValuesKStream = jobKStream.mapValues(valueMapper);
        KStream<String, DetailedJob> mapValuesKStreamWithNamed = jobKStream.mapValues(valueMapper, Named.as("map-with-values-stream"));
        KStream<String, DetailedJob> mapValuesKStreamWithKey = jobKStream.mapValues(valueMapperWithKey);
        KStream<String, DetailedJob> mapValuesKStreamWithNamedWithKey = jobKStream.mapValues(valueMapperWithKey, Named.as("map-with-values-stream-with-key"));

        KTable<String, DetailedJob> mapValuesKTable = jobKTable.mapValues(valueMapper);
        KTable<String, DetailedJob> mapValuesKTableWithNamed = jobKTable.mapValues(valueMapper, Named.as("map-with-values-table"));
        KTable<String, DetailedJob> mapValuesKTableWithKey = jobKTable.mapValues(valueMapperWithKey);
        KTable<String, DetailedJob> mapValuesKTableWithNamedWithKey = jobKTable.mapValues(valueMapperWithKey, Named.as("map-with-values-table-with-key"));
        KTable<String, DetailedJob> mapValuesKTableWithView = jobKTable.mapValues(valueMapper, detailedJobView);
        KTable<String, DetailedJob> mapValuesKTableWithNamedWithView = jobKTable.mapValues(valueMapper, Named.as("map-with-values-table-with-view"), detailedJobView);
        KTable<String, DetailedJob> mapValuesKTableWithKeyWithView = jobKTable.mapValues(valueMapperWithKey, detailedJobView);
        KTable<String, DetailedJob> mapValuesKTableWithNamedWithKeyWithView = jobKTable.mapValues(valueMapperWithKey, Named.as("map-with-values-table-with-key-with-view"), detailedJobView);

        mapKStream.to("mapKStream", produced);
        mapKStreamWithNamed.to("mapKStreamWithNamed", produced);
        mapValuesKStream.to("mapValuesKStream", produced);
        mapValuesKStreamWithNamed.to("mapValuesKStreamWithNamed", produced);
        mapValuesKStreamWithKey.to("mapValuesKStreamWithKey", produced);
        mapValuesKStreamWithNamedWithKey.to("mapValuesKStreamWithNamedWithKey", produced);
        mapValuesKTable.toStream().to("mapValuesKTable", produced);
        mapValuesKTableWithNamed.toStream().to("mapValuesKTableWithNamed", produced);
        mapValuesKTableWithKey.toStream().to("mapValuesKTableWithKey", produced);
        mapValuesKTableWithNamedWithKey.toStream().to("mapValuesKTableWithNamedWithKey", produced);
        mapValuesKTableWithView.toStream().to("mapValuesKTableWithView", produced);
        mapValuesKTableWithNamedWithView.toStream().to("mapValuesKTableWithNamedWithView", produced);
        mapValuesKTableWithKeyWithView.toStream().to("mapValuesKTableWithKeyWithView", produced);
        mapValuesKTableWithNamedWithKeyWithView.toStream().to("mapValuesKTableWithNamedWithKeyWithView", produced);
        return builder.build();
    }
}
