package by.jdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static by.jdev.LowLevelProducer.TOPIC;
/**
 * Координатор группы расположен на брокере и контролирует сердцебиения и запросы на вычитку из очереди.
 * Лидер группы- 1ый подключившийся потребитель, который выполняет перераспределение партиций на участников группы.
 * */

public class LowLevelConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        /**
         * Если при создании потребителя не указать идентификатор группы, то будет создан особый тип потребителя — автономный потребитель (standalone consumer).
         Ключевое отличие таких потребителей — вместо того, чтобы полагаться на механизмы Kafka в управлении своим состоянием, они полностью отдают это на откуп пользовательскому коду.*/
        props.setProperty("group.id", "group1");

        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        props.setProperty("fetch.min.bytes", "1"); // меньше порция данных
        props.setProperty("fetch.max.wait.ms", "50"); // меньше ожидание

        props.setProperty("fetch.min.bytes", "65536");   // 64 КБ
        props.setProperty("fetch.max.wait.ms", "1000");  // до 1 секунды ожидания

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));// указывает сколько ждать, если сообщений нет

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}

