import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws UnsupportedEncodingException {
        {
            Properties prop = new Properties();
            //  prop.put("zookeeper.connect", "zk1:2181,zk2:2181,zk3:2181");
            prop.put("bootstrap.servers", "broker:9092");
//            prop.put("serializer.class", StringEncoder.class.getName());
            prop.put("serializer.class", "kafka.serializer.StringDeserializer");
            prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put("auto.offset.reset", "smallest");

            prop.put("group.id", "777");
//            String topic = "vr_video";
            String topic = "__consumer_offsets";
            //String topic ="youju_event";
            ConsumerConnector cons = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = cons.createMessageStreams(topicCountMap);
            final KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get(topic).get(0);
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            System.out.println("开始");
            while (iterator.hasNext()) {
                if (iterator.next() != null) {
                    System.out.println("offset: " + iterator.next().offset());
                    System.out.println("key: " + Arrays.toString(iterator.next().key()));
                    System.out.println("copy$default$1: " + iterator.next().copy$default$1());
                    System.out.println("copy$default$2: " + iterator.next().copy$default$2());
                    System.out.println("copy$default$3: " + iterator.next().copy$default$3());
                    System.out.println("copy$default$4: " + iterator.next().copy$default$4());
                    System.out.println("copy$default$5: " + iterator.next().copy$default$5());
                    System.out.println("copy$default$6: " + iterator.next().copy$default$6());
                    System.out.println("productPrefix: " + iterator.next().productPrefix());
                    System.out.println("valueDecoder: " + iterator.next().valueDecoder());
                    System.out.println("keyDecoder: " + iterator.next().keyDecoder());
                    System.out.println("topic: " + iterator.next().topic());
                    System.out.println("productIterator: " + iterator.next().productIterator());
                    System.out.println("partition: " + iterator.next().partition());
                    System.out.println("productArity: " + iterator.next().productArity());
                    System.out.println("next: " + iterator.next().toString());
                    String msg = new String(iterator.next().message(), "UTF-8");
                }
            }
        }
    }
}
