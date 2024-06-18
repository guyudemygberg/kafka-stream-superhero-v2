package best.of.kafka.streams.joins;

import best.of.kafka.streams.dto.Coupon;
import best.of.kafka.streams.dto.JoinedClass;
import best.of.kafka.streams.dto.Product;
import best.of.kafka.streams.utils.KafkaSerdeUtils;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

@Component
public class KTableToKTable {

    Gson gson = new Gson();

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Product> productKTable = builder.table("product", Consumed.with(Serdes.String(), KafkaSerdeUtils.getSerde(Product.class, gson)));
        KTable<String, Coupon> couponKTable = builder.table("coupon", Consumed.with(Serdes.String(), KafkaSerdeUtils.getSerde(Coupon.class, gson)));
        ValueJoiner<Product, Coupon, JoinedClass> joiner = (product, coupon) -> new JoinedClass(product.getName(), coupon == null ? null : coupon.getDiscount());
        Materialized<String, JoinedClass, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as("sjs").with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson));

        KTable<String, JoinedClass> innerJoinValueJoiner = productKTable.join(couponKTable, joiner);
        KTable<String, JoinedClass> innerJoinValueJoinerNamed = productKTable.join(couponKTable, joiner, Named.as("innerJoinValueJoinerNamed"));
        KTable<String, JoinedClass> innerJoinValueJoinerMaterialized = productKTable.join(couponKTable, joiner, materialized);
        KTable<String, JoinedClass> innerJoinValueJoinerNamedMaterialized = productKTable.join(couponKTable, joiner, Named.as("innerJoinValueJoinerNamedMaterialized"), materialized);
        KTable<String, JoinedClass> leftJoinValueJoiner = productKTable.leftJoin(couponKTable, joiner);
        KTable<String, JoinedClass> leftJoinValueJoinerNamed = productKTable.leftJoin(couponKTable, joiner, Named.as("leftJoinValueJoinerNamed"));
        KTable<String, JoinedClass> leftJoinValueJoinerMaterialized = productKTable.leftJoin(couponKTable, joiner, materialized);
        KTable<String, JoinedClass> leftJoinValueJoinerNamedMaterialized = productKTable.leftJoin(couponKTable, joiner, Named.as("leftJoinValueJoinerNamedMaterialized"), materialized);
        KTable<String, JoinedClass> outerJoinValueJoiner = productKTable.outerJoin(couponKTable, joiner);
        KTable<String, JoinedClass> outerJoinValueJoinerNamed = productKTable.outerJoin(couponKTable, joiner, Named.as("outerJoinValueJoinerNamed"));
        KTable<String, JoinedClass> outerJoinValueJoinerMaterialized = productKTable.outerJoin(couponKTable, joiner, materialized);
        KTable<String, JoinedClass> outerJoinValueJoinerNamedMaterialized = productKTable.outerJoin(couponKTable, joiner, Named.as("outerJoinValueJoinerNamedMaterialized"), materialized);

        Produced<String, JoinedClass> producedWith = Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson));

        innerJoinValueJoiner.toStream().to("innerJoinValueJoiner", producedWith);
        innerJoinValueJoinerNamed.toStream().to("innerJoinValueJoinerNamed", producedWith);
        innerJoinValueJoinerMaterialized.toStream().to("innerJoinValueJoinerMaterialized", producedWith);
        innerJoinValueJoinerNamedMaterialized.toStream().to("innerJoinValueJoinerNamedMaterialized", producedWith);
        leftJoinValueJoiner.toStream().to("leftJoinValueJoiner", producedWith);
        leftJoinValueJoinerNamed.toStream().to("leftJoinValueJoinerNamed", producedWith);
        leftJoinValueJoinerMaterialized.toStream().to("leftJoinValueJoinerMaterialized", producedWith);
        leftJoinValueJoinerNamedMaterialized.toStream().to("leftJoinValueJoinerNamedMaterialized", producedWith);
        outerJoinValueJoiner.toStream().to("outerJoinValueJoiner", producedWith);
        outerJoinValueJoinerNamed.toStream().to("outerJoinValueJoinerNamed", producedWith);
        outerJoinValueJoinerMaterialized.toStream().to("outerJoinValueJoinerMaterialized", producedWith);
        outerJoinValueJoinerNamedMaterialized.toStream().to("outerJoinValueJoinerNamedMaterialized", producedWith);


        return builder.build();
    }
}
