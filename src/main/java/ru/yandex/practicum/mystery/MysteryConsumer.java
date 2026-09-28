package ru.yandex.practicum.mystery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MysteryConsumer {

    public static void main(String[] args) {
        String baseConsumerName = "mystery-consumer-new"; //args[0];
        int numberOfConsumers = 3; //Integer.parseInt(args[1]);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "mystery-consumers");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("auto.commit.interval.ms", "20000");

        try (ExecutorService executor = Executors.newFixedThreadPool(numberOfConsumers)) {
            for (int num = 1; num <= numberOfConsumers; num++) {
                String consumerName = baseConsumerName + "-" + num;
                executor.submit(
                        () -> {
                            //потребитель не-потокобезопасный, поэтому создаем каждого в своем потоке
                            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

                                consumer.subscribe(List.of("kafka-mystery"));

                                System.out.println("Создан потребитель с именем " + consumerName);

                                while (true) {
                                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//                                    System.out.println("Новая итерация цикла для потребителя " + consumerName);
                                    for (ConsumerRecord<String, String> consumerRecord : records) {
                                        long timeToReceive = System.currentTimeMillis() - consumerRecord.timestamp();
                                        System.out.printf("Потребитель: %s, сообщение: %s, ключ: %s, номер партиции: %d, офсет: %d, время на доставку: %d%n",
                                                consumerName,
                                                consumerRecord.value(),
                                                consumerRecord.key(),
                                                consumerRecord.partition(),
                                                consumerRecord.offset(),
                                                timeToReceive
                                        );
                                    }
                                }
                            }

                        }
                );
            }
        }

    }
}
