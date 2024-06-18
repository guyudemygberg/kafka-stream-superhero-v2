package best.of.kafka.streams.joins;

import best.of.kafka.streams.dto.Coupon;
import best.of.kafka.streams.dto.JoinedClass;
import best.of.kafka.streams.dto.Product;
import best.of.kafka.streams.utils.KafkaSerdeUtils;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class KStreamToKTable {

    Gson gson = new Gson();

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Product> productStream = builder.stream("product", Consumed.with(Serdes.String(), KafkaSerdeUtils.getSerde(Product.class, gson)));
        KTable<String, Coupon> couponStream = builder.table("coupon", Consumed.with(Serdes.String(), KafkaSerdeUtils.getSerde(Coupon.class, gson)));
        KStream<String, String> productStringStream = productStream.mapValues((product) -> product.getName());
        KTable<String, String> couponStringStream = couponStream.mapValues((coupon) -> coupon.getDiscount() + "");
        Joined joined = Joined.with(Serdes.String(), KafkaSerdeUtils.getSerde(Product.class, gson), KafkaSerdeUtils.getSerde(Coupon.class, gson));

        KStream<String, JoinedClass> valueJoinerNoDefault =   productStream.join(couponStream, (product, coupon) -> new JoinedClass(product.getName(), coupon.getDiscount()), joined);
        KStream<String, JoinedClass> valueJoinerWithDefault = productStringStream.join(couponStringStream, (product, coupon) -> new JoinedClass(product, Integer.valueOf(coupon)));
        KStream<String, JoinedClass> keyValueJoinerNoDefault =   productStream.join(couponStream, (key, product, coupon) -> new JoinedClass(key + product.getName(), coupon.getDiscount()), joined);
        KStream<String, JoinedClass> keyValueJoinerWithDefault = productStringStream.join(couponStringStream, (key, product, coupon) -> new JoinedClass(key + product, Integer.valueOf(coupon)));

        KStream<String, JoinedClass> leftValueJoinerNoDefault =   productStream.leftJoin(couponStream, (product, coupon) -> new JoinedClass(product.getName(), coupon == null ? null : coupon.getDiscount()), joined);
        KStream<String, JoinedClass> leftValueJoinerWithDefault = productStringStream.leftJoin(couponStringStream, (product, coupon) -> new JoinedClass(product, coupon == null ? null : Integer.valueOf(coupon)));
        KStream<String, JoinedClass> leftKeyValueJoinerNoDefault =   productStream.leftJoin(couponStream, (key, product, coupon) -> new JoinedClass(key + product.getName(), coupon == null ? null : coupon.getDiscount()), joined);
        KStream<String, JoinedClass> leftKeyValueJoinerWithDefault = productStringStream.leftJoin(couponStringStream, (key, product, coupon) -> new JoinedClass(key + product, coupon == null ? null : Integer.valueOf(coupon)));

        valueJoinerNoDefault.to("innerJoin-ValueJoiner", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));
        valueJoinerWithDefault.to("innerJoin-ValueJoiner-default", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));
        keyValueJoinerNoDefault.to("innerJoin-KeyValueJoiner", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));
        keyValueJoinerWithDefault.to("innerJoin-KeyValueJoiner-default", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));
        leftValueJoinerNoDefault.to("leftJoin-ValueJoiner", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));
        leftValueJoinerWithDefault.to("leftJoin-ValueJoiner-default", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));
        leftKeyValueJoinerNoDefault.to("leftJoin-KeyValueJoiner", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));
        leftKeyValueJoinerWithDefault.to("leftJoin-KeyValueJoiner-default", Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class, gson)));

        return builder.build();
    }
}
