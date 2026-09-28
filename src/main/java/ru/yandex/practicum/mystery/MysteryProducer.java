package ru.yandex.practicum.mystery;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;


public class MysteryProducer {

    public static void main(String[] args) {
        String baseProducerName = "mystery-producer";//args[0];
        int numberOfProducers = 3; //Integer.parseInt(args[1]);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.setProperty("batch.size", "131072");
//        props.setProperty("linger.ms", "2000");

        //производитель потокобезопасный, поэтому создаем один экземпляр на все потоки
        Producer<String, String> producer = new KafkaProducer<>(props);

        ExecutorService executor = Executors.newFixedThreadPool(numberOfProducers);
        for (int num = 1; num <= numberOfProducers; num++) {
            String producerName = baseProducerName + "-" + num;
            executor.submit(() -> {
                System.out.println("Создан производитель с именем " + producerName);

                while (true) {
                    try {
                        //генерируем индекс для ключа, от 1 о 10
                        int keyIdx = ThreadLocalRandom.current().nextInt(0, 10) + 1;
                        //создаем ключ
                        String messageKey = "key" + keyIdx;

                        //генерируем магическое число от 0 до 100
                        int magicNumber = ThreadLocalRandom.current().nextInt(0, 100);
                        //собираем сообщение
                        String messageValue = producerName + " " + magicNumber;

                        //создаем сообщение. Передаем null в качестве номера партиции - она должна определяться по ключу
                        ProducerRecord<String, String> message =
                                new ProducerRecord<>("kafka-mystery", null, System.currentTimeMillis(), messageKey, messageValue);
                        producer.send(message);

                        //Определяем паузу перед отправкой следующего сообщения - от 0 до 2 секунд
                        int timeToWait = ThreadLocalRandom.current().nextInt(0, 2000);
                        Thread.sleep(timeToWait);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
