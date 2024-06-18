package best.of.kafka.streams.topology;

import best.of.kafka.streams.processor.SimpleProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class ProcessorTopology {

//    @Bean
    public Topology createTopology(){
      Topology topology = new Topology();
      topology.addSource("Source", "disney-quotes", "basketball-quotes")
              .addProcessor("Processor", new SimpleProcessor(),"Source")
              .addSink("Sink", "processorTopic", "Processor");

      return topology;
    }
}
