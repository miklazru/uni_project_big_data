package ru.hse.bigdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // 1. Инициализация окружений
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. Настройка источника Kafka (внутренний адрес для Docker)
        KafkaSource<IoTEvent> kafkaSource = KafkaSource.<IoTEvent>builder()
                .setBootstrapServers("kafka:9092") 
                .setTopics("iot-raw-events")
                .setGroupId("flink-analytics-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(IoTEvent.class))
                .build();

        // 3. Стратегия Watermark (Event Time)
        WatermarkStrategy<IoTEvent> watermarkStrategy = WatermarkStrategy
                .<IoTEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> java.time.LocalDateTime.parse(event.event_time, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(ZoneOffset.UTC).toEpochMilli());

        DataStream<IoTEvent> stream = env.fromSource(kafkaSource, watermarkStrategy, "Kafka IoT Source");

        // 4. Оконная агрегация (Расчет среднего и медианы)
        DataStream<WindowResult> aggregatedStream = stream
                .keyBy(event -> event.id)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new MetricsAggregator(), new WindowResultProcessor());

        // 5. ИСПРАВЛЕНИЕ: Переход в Table API с явным маппингом типов и добавлением PROCTIME()
        Table statsTable = tableEnv.fromDataStream(
                aggregatedStream,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("windowEnd", DataTypes.TIMESTAMP(3)) // Лечим ошибку 'OTHER'
                        .column("avgTemperature", DataTypes.DOUBLE())
                        .column("medianHumidity", DataTypes.DOUBLE())
                        .columnByExpression("proc_time", "PROCTIME()") // Добавляем поле для Lookup JOIN
                        .build()
        );
        tableEnv.createTemporaryView("window_stats", statsTable);

        // 6. Регистрация справочника PostgreSQL (внутренний адрес для Docker)
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE device_types (" +
                "  id INT," +
                "  type_name STRING," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:postgresql://postgres:5432/iot_db'," +
                "  'table-name' = 'iot_device_types'," +
                "  'username' = 'postgres'," +
                "  'password' = 'password123'" +
                ")"
        );

        // 7. Регистрация выходного топика Kafka (Sink)
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE kafka_sink (" +
                "  `time` STRING," +
                "  `type_name` STRING," +
                "  `avg_temperature` DOUBLE," +
                "  `median_humidity` DOUBLE" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'iot-processed-results'," +
                "  'properties.bootstrap.servers' = 'kafka:9092'," +
                "  'format' = 'json'" +
                ")"
        );

        // 8. ИСПРАВЛЕНИЕ: Делаем Lookup JOIN по валидному атрибуту времени ws.proc_time
        tableEnv.executeSql(
                "INSERT INTO kafka_sink " +
                "SELECT " +
                "  DATE_FORMAT(ws.windowEnd, 'HH:mm') as `time`, " +
                "  dt.type_name, " +
                "  ws.avgTemperature, " +
                "  ws.medianHumidity " +
                "FROM window_stats ws " +
                "JOIN device_types FOR SYSTEM_TIME AS OF ws.proc_time AS dt " +
                "ON ws.id = dt.id"
        );
    }

    public static class WindowAccumulator {
        long count = 0;
        double sumTemp = 0;
        List<Double> humidities = new ArrayList<>();
    }

    public static class MetricsAggregator implements AggregateFunction<IoTEvent, WindowAccumulator, WindowAccumulator> {
        @Override
        public WindowAccumulator createAccumulator() { return new WindowAccumulator(); }

        @Override
        public WindowAccumulator add(IoTEvent value, WindowAccumulator accum) {
            accum.count++;
            accum.sumTemp += value.temperature;
            accum.humidities.add(value.humidity);
            return accum;
        }

        @Override
        public WindowAccumulator getResult(WindowAccumulator accum) { return accum; }

        @Override
        public WindowAccumulator merge(WindowAccumulator a, WindowAccumulator b) {
            a.count += b.count;
            a.sumTemp += b.sumTemp;
            a.humidities.addAll(b.humidities);
            return a;
        }
    }

    public static class WindowResultProcessor extends ProcessWindowFunction<WindowAccumulator, WindowResult, Integer, TimeWindow> {
        @Override
        public void process(Integer id, Context context, Iterable<WindowAccumulator> elements, Collector<WindowResult> out) {
            WindowAccumulator acc = elements.iterator().next();
            double avgTemp = acc.sumTemp / acc.count;

            Collections.sort(acc.humidities);
            double medianHumidity;
            int size = acc.humidities.size();
            if (size % 2 == 0) {
                medianHumidity = (acc.humidities.get(size / 2 - 1) + acc.humidities.get(size / 2)) / 2.0;
            } else {
                medianHumidity = acc.humidities.get(size / 2);
            }

            java.time.LocalDateTime windowEnd = java.time.LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(context.window().getEnd()), java.time.ZoneId.of("UTC"));

            out.collect(new WindowResult(id, windowEnd, Math.round(avgTemp * 100.0) / 100.0, Math.round(medianHumidity * 100.0) / 100.0));
        }
    }
}
