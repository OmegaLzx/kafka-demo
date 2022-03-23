package com.jc.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author zexuan.Li  2022/3/21
 */
@Slf4j
public class CustomProducerCallback {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "jc-team3-02:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 5000; i++) {
            producer.send(new ProducerRecord<>("test", "value" + i), (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
                log.info("分区 {}, 位移 {}", metadata.partition(), metadata.offset());
            });
        }

        producer.close();
    }
}
