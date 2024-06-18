package best.of.kafka.streams.kstreamktablemethods;

import best.of.kafka.streams.dto.People;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class FilterAndFilterNot {

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, People> peopleKStream = builder.stream("PeopleKStream", Consumed.with(Serdes.String(), SerdesUtils.getSerde(People.class)));
        KTable<String, People> peopleKTable = builder.table("PeopleKTable", Consumed.with(Serdes.String(), SerdesUtils.getSerde(People.class)));

        Predicate<String, People> isInCustomFilter = (key, value) -> value.isCostumed();
        Predicate<String, People> isWearingHatFilter = (key, value) -> value.isWearingHat();
        Materialized<String, People, KeyValueStore<Bytes, byte[]>> peopleView = Materialized.as("people-view").with(Serdes.String(), SerdesUtils.getSerde(People.class));
        Produced<String, People> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(People.class));

        KStream<String, People> customParty = peopleKStream.filter(isInCustomFilter);
        KStream<String, People> customPartyWithName = peopleKStream.filter(isInCustomFilter, Named.as("custom-party-stream"));
        KStream<String, People> noHatParty = peopleKStream.filterNot(isWearingHatFilter);
        KStream<String, People> noHatPartyWithName = peopleKStream.filterNot(isWearingHatFilter, Named.as("no-hat-party-stream"));

        KTable<String, People> customPartyTable = peopleKTable.filter(isInCustomFilter);
        KTable<String, People> customPartyTableWithName = peopleKTable.filter(isInCustomFilter, Named.as("custom-party-table"));
        KTable<String, People> noHatPartyTable = peopleKTable.filterNot(isWearingHatFilter);
        KTable<String, People> noHatPartyTableWithName = peopleKTable.filterNot(isWearingHatFilter, Named.as("no-hat-party-table"));
        KTable<String, People> customPartyTableWithView = peopleKTable.filter(isInCustomFilter, peopleView);
        KTable<String, People> customPartyTableWithNameWithView = peopleKTable.filter(isInCustomFilter, Named.as("custom-party-table-with-view"), peopleView);
        KTable<String, People> noHatPartyTableWithView = peopleKTable.filterNot(isWearingHatFilter, peopleView);
        KTable<String, People> noHatPartyTableWithNameWithView = peopleKTable.filterNot(isWearingHatFilter, Named.as("no-hat-party-table-with-view"), peopleView);

        customParty.to("customParty", produced);
        customPartyWithName.to("customPartyWithName", produced);
        noHatParty.to("noHatParty", produced);
        noHatPartyWithName.to("noHatPartyWithName", produced);
        customPartyTable.toStream().to("customPartyTable", produced);
        customPartyTableWithName.toStream().to("customPartyTableWithName", produced);
        noHatPartyTable.toStream().to("noHatPartyTable", produced);
        noHatPartyTableWithName.toStream().to("noHatPartyTableWithName", produced);
        customPartyTableWithView.toStream().to("customPartyTableWithView", produced);
        customPartyTableWithNameWithView.toStream().to("customPartyTableWithNameWithView", produced);
        noHatPartyTableWithView.toStream().to("noHatPartyTableWithView", produced);
        noHatPartyTableWithNameWithView.toStream().to("noHatPartyTableWithNameWithView", produced);

        return builder.build();
    }
}
