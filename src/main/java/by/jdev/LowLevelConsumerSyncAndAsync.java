package by.jdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static by.jdev.LowLevelProducer.TOPIC;


public class LowLevelConsumerSyncAndAsync {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "group1");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            record.value()
                    );
                }
                /*Для использования commitSync/Async значение параметра необходимо enable.auto.commit=false. */
                consumer.commitSync(Duration.ofMillis(200)); //максимальное время ожидания успешного коммита, для случая повторяемой ошибки.
                // Также для настройки тайм-аута можно использовать параметр потребителя default.api.timeout.ms.

                //У метода commitAsync также есть версия, которая принимает на вход коллбэк-метод. Этот метод будет вызван после завершения коммита — успешно или с ошибкой.
            }
        }

    }
}

