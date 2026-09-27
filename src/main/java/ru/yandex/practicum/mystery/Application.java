package ru.yandex.practicum.mystery;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

public class Application {

    static class TraceProvider implements Supplier<String> {

        @Override
        public String get() {
            // ... Имитация получения traceId.
            // В настоящем приложении можно использовать, например, ThreadLocal.

            return UUID.randomUUID().toString();
        }
    }

    static class TraceProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

        public static final String TRACE_ID_HEADER = "x-trace-id";

        private final Supplier<String> traceProvider;

        public TraceProducerInterceptor(Supplier<String> traceProvider) {
            this.traceProvider = traceProvider;
        }

        @Override
        public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
            Header traceHeader = record.headers().lastHeader(TRACE_ID_HEADER);
            if (traceHeader != null) {
                // Если заголовок уже присутствует, пропускаем этот шаг.
                return record;
            }

            // Добавляем к заголовкам полученный traceId.
            Headers newHeaders = record.headers().add(new RecordHeader(TRACE_ID_HEADER, traceProvider.get().getBytes()));

            // Дублируем сообщение, но с заголовком.
            return new ProducerRecord<>(
                    record.topic(),
                    record.partition(),
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    newHeaders
            );
        }

        // Остальные методы можно оставить без изменения.

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    public static void main(String[] args) {
        TraceProvider traceProvider = new TraceProvider();
        ProducerInterceptor<String, String> producerInterceptor = new TraceProducerInterceptor<>(traceProvider);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "topic.test",
                0,
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        Header header = extractTrace(producerInterceptor, record);
        System.out.println("header  = " + header);

        ProducerRecord<String, String> recordWithTrace = new ProducerRecord<>(
                "topic.test",
                0,
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                List.of(new RecordHeader(TraceProducerInterceptor.TRACE_ID_HEADER, UUID.fromString("b6c46352-e513-47df-ad0b-5d85181106c0").toString().getBytes()))
        );
        Header onSend = extractTrace(producerInterceptor, recordWithTrace);
        if (onSend != null) {
            System.out.println(new String(onSend.value()));
        }
    }

    static Header extractTrace(ProducerInterceptor<String, String> producerInterceptor, ProducerRecord<String, String> record) {
        ProducerRecord<String, String> traceRecord = producerInterceptor.onSend(record);
        Header traceHeader = traceRecord.headers().lastHeader(TraceProducerInterceptor.TRACE_ID_HEADER);
        System.out.println("traceHeader = " + traceHeader);
        if (traceHeader == null) {
            System.err.println("Отсутствует заголовок " + TraceProducerInterceptor.TRACE_ID_HEADER);
        }

        return traceHeader;
    }

}