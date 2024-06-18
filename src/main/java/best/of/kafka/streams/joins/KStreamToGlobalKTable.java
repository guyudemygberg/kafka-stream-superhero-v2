package best.of.kafka.streams.joins;

import best.of.kafka.streams.dto.Cart;
import best.of.kafka.streams.dto.CartJoinedClass;
import best.of.kafka.streams.dto.JoinedClass;
import best.of.kafka.streams.utils.KafkaSerdeUtils;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

//@Component
public class KStreamToGlobalKTable {

    Gson gson = new Gson();

    //@Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JoinedClass> joinedClassKStream = builder.stream("joined-class", Consumed.with(Serdes.String(), KafkaSerdeUtils.getSerde(JoinedClass.class,gson)));
        GlobalKTable<String, Cart> cartGlobalKTable = builder.globalTable("cart", Consumed.with(Serdes.String(), KafkaSerdeUtils.getSerde(Cart.class,gson)));
        KeyValueMapper<String, JoinedClass, String> keyValueMapper = (joinedClassKey, joinedClassValue) -> joinedClassValue.getName();
        ValueJoiner<JoinedClass,Cart, CartJoinedClass> valueJoiner = (joinedClass, cart) -> new CartJoinedClass(joinedClass, cart);
        ValueJoinerWithKey<String, JoinedClass, Cart, CartJoinedClass> valueJoinerWithKey = (key, joinedClass, cart) -> new CartJoinedClass(key, joinedClass, cart);
        joinedClassKStream.peek((key, value) -> System.out.println(key + value));

        KStream<String, CartJoinedClass> innerValueJoin = joinedClassKStream.join(cartGlobalKTable, keyValueMapper, valueJoiner);
        KStream<String, CartJoinedClass> innerValueJoinWithKey = joinedClassKStream.join(cartGlobalKTable, keyValueMapper, valueJoinerWithKey);
        KStream<String, CartJoinedClass> innerValueJoinWithName = joinedClassKStream.join(cartGlobalKTable, keyValueMapper, valueJoiner, Named.as("inner-valueJoiner"));
        KStream<String, CartJoinedClass> innerValueJoinWithKeyWithName = joinedClassKStream.join(cartGlobalKTable, keyValueMapper, valueJoinerWithKey, Named.as("inner-valueJoinerWithKey"));
        KStream<String, CartJoinedClass> leftValueJoin = joinedClassKStream.leftJoin(cartGlobalKTable, keyValueMapper, valueJoiner);
        KStream<String, CartJoinedClass> leftValueJoinWithKey = joinedClassKStream.leftJoin(cartGlobalKTable, keyValueMapper, valueJoinerWithKey);
        KStream<String, CartJoinedClass> leftValueJoinWithName = joinedClassKStream.leftJoin(cartGlobalKTable, keyValueMapper, valueJoiner, Named.as("left-valueJoiner"));
        KStream<String, CartJoinedClass> leftValueJoinWithKeyWithName = joinedClassKStream.leftJoin(cartGlobalKTable, keyValueMapper, valueJoinerWithKey, Named.as("left-valueJoinerWithKey"));

        Produced<String, CartJoinedClass> produced = Produced.with(Serdes.String(), KafkaSerdeUtils.getSerde(CartJoinedClass.class, gson));
        innerValueJoin.to("innerValueJoin", produced);
        innerValueJoinWithKey.to("innerValueJoinWithKey", produced);
        innerValueJoinWithName.to("innerValueJoinWithName", produced);
        innerValueJoinWithKeyWithName.to("innerValueJoinWithKeyWithName", produced);
        leftValueJoin.to("leftValueJoin", produced);
        leftValueJoinWithKey.to("leftValueJoinWithKey", produced);
        leftValueJoinWithName.to("leftValueJoinWithName", produced);
        leftValueJoinWithKeyWithName.to("leftValueJoinWithKeyWithName", produced);

        return  builder.build();
    }
}
