package by.jdev;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
/**
 *  Квотирование, позволяет ограничить нагрузку на кластер Kafka или отдельные брокеры.
 *  Можно ограничить количество поступающих запросов от каждого конкретного производителя, используя параметр quota.producer.default.
 *  quota.producer.default=2 означает, что запросы от производителей могут поступать со скоростью не больше, чем 2 мегабита в секунду.
* */
public class LowLevelProducer {

    public static final String TOPIC = "NewVisitors";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties propsForProducer = getProperties();

        Producer<String, String> producerZero = new KafkaProducer<>(propsForProducer);
        producerZero.send(new ProducerRecord<>(TOPIC, "visitor1"));
        producerZero.close();

        try (Producer<String, String> producer = new KafkaProducer<>(propsForProducer)) {

            ProducerRecord<String, String> recWithAllParams = new ProducerRecord<>(TOPIC, 1, System.currentTimeMillis(), "someKey", "Some Visitor");
            recWithAllParams.headers().add(
                    "my-header",
                    "my-value".getBytes(StandardCharsets.UTF_8)
            );
            ProducerRecord<String, String> recWithMinimumParams = new ProducerRecord<>(TOPIC, "visitor2");
            /* тип отправки: (по возможности) Fire and forget */
            producer.send(recWithMinimumParams);

            /* тип отправки: синхронный - дожидаемся выполнения метода get for Future. */
            Future<RecordMetadata> recordMetadataFuture = producer.send(recWithAllParams);
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            printRecordMetadata(recordMetadata);

            /* тип отправки: АСИНХРОННЫЙ, добавляется параметр функция-обработчик ответа, который придет после отправки в брокер*/
            producer.send(recWithMinimumParams, (recMetadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
                System.out.println("Async case:");
                printRecordMetadata(recMetadata);
            });

            /* Смотрим как сообщения распределяются по разделам */
            for (int i=0; i<30; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "key1", "visitor" + i + "_1"));
                producer.send(new ProducerRecord<>(TOPIC, "key2", "visitor" + i + "_2"));
                producer.send(new ProducerRecord<>(TOPIC, "key3", "visitor" + i + "_3"));
            }

            /* Типы ошибок при отправке:
            * Повторяемые — которые может решить повторная отправка (например, недоступность кластера — он может через какое-то время стать доступным).
            * Неповторяемая — например, слишком большой размер сообщения: нет смысла пытаться отправить такое сообщение повторно.
            * */
        }

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        props.put("acks", "1");
        /** acks:
         *  0 говорит, что производитель вообще не будет ждать ответа от сервера и считает сообщение сохранённым, как только оно отправлено брокеру.
         *  1 означает, что достаточно, чтобы сообщение было сохранено только тем брокером, к которому был выполнен запрос, а дальнейшая его репликация остаётся на совести кластера Kafka.
         * -1 (or all) сообщение должно быть сохранено на всех имеющихся репликах, прежде чем производитель получит подтверждение его сохранения. Это самый медленный и самый надёжный вариант.
         * В последних версиях Kafka по умолчанию используется значение -1
         */return props;
    }

    private static void printRecordMetadata(RecordMetadata recordMetadata) {
        System.out.println(
                "recordMetadata.partition = " + recordMetadata.partition() + "\n" +
                "recordMetadata.offset = " + recordMetadata.offset()
        );
    }

}
