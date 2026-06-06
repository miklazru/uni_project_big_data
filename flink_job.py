import os
from pyflink.common import WatermarkStrategy, Time, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.table import StreamTableEnvironment, DataTypes
import statistics

# --- 1. Пользовательская функция для расчета Медианы и Среднего в окне ---
class AggregateMetricsFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        # elements - это итератор строк, попавших в 1-минутное окно
        temperatures = []
        humidities = []
        
        device_id = key[0] # Так как мы делали key_by по id
        
        for row in elements:
            # Извлекаем температуру и влажность из Row
            temperatures.append(row[2])
            num_humidity = row[3]
            if num_humidity is not None:
                humidities.append(num_humidity)
        
        # Считаем метрики за минуту
        avg_temp = round(sum(temperatures) / len(temperatures), 2) if temperatures else 0.0
        median_hum = round(statistics.median(humidities), 2) if humidities else 0.0
        
        # Получаем время окончания окна и форматируем в hh:mm по условию
        window_end_ms = context.window().end()
        # Переводим миллисекунды в формат "HH:mm" (напр. "14:35")
        from datetime import datetime, timezone
        time_str = datetime.fromtimestamp(window_end_ms / 1000.0, tz=timezone.utc).strftime('%H:%M')
        
        # Возвращаем результат: (time_hh_mm, device_id, avg_temp, median_hum)
        yield (time_str, device_id, avg_temp, median_hum)


def run_job():
    # --- 2. Инициализация окружения ---
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    t_env = StreamTableEnvironment.create(env)

    # Автоматически загружаем необходимые jar-пакеты для Kafka и Postgres
    kafka_jar = "org.apache.flink:flink-connector-kafka:3.0.2-1.18"
    jdbc_jar = "org.apache.flink:flink-connector-jdbc:3.1.2-1.18"
    postgres_jar = "org.postgresql:postgresql:42.6.0"
    
    env.add_jars(
        f"maven://{kafka_jar}",
        f"maven://{jdbc_jar}",
        f"maven://{postgres_jar}"
    )

    # --- 3. SQL API: Создание Источника Kafka (Sorce) ---
    t_env.execute_sql("""
        CREATE TABLE kafka_raw_src (
            id INT,
            event_time TIMESTAMP(3),
            temperature DOUBLE,
            humidity DOUBLE,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iot-raw-events',
            'properties.bootstrap.servers' = 'localhost:29092',
            'properties.group.id' = 'flink-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # --- 4. Переход из SQL API в DataStream API ---
    # Переводим таблицу в DataStream для кастомной оконной аналитики (расчет медианы)
    table_src = t_env.from_path("kafka_raw_src")
    ds_src = t_env.to_data_stream(table_src)

    # --- 5. DataStream API: Оконная обработка (Event Time) ---
    # Извлекаем Timestamp и Watermark, которые настроили в SQL
    class EventTimeAssigner(TimestampAssigner):
        def extract_timestamp(self, element, record_timestamp):
            # Переводим объект лога в миллисекунды эпохи Unix
            return int(element[1].cast_to_row_time())

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(EventTimeAssigner())

    processed_stream = ds_src \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda row: row[0], key_type=Types.ROW([Types.INT()])) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .process(AggregateMetricsFunction(), 
                 output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.DOUBLE(), Types.DOUBLE()]))

    # --- 6. Переход обратно из DataStream API в SQL API ---
    # Создаем временное представление (таблицу) из посчитанного потока
    result_table = t_env.from_data_stream(
        processed_stream, 
        ['window_time', 'device_id', 'avg_temperature', 'median_humidity']
    )
    t_env.create_temporary_view("aggregated_metrics", result_table)

    # --- 7. SQL API: Подключение Справочника Postgres ---
    t_env.execute_sql("""
        CREATE TABLE postgres_lookup (
            id INT,
            type_name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/iot_db',
            'table-name' = 'iot_device_types',
            'username' = 'postgres',
            'password' = 'password123'
        )
    """)

    # --- 8. SQL API: Создание Приемника Kafka (Sink) ---
    t_env.execute_sql("""
        CREATE TABLE kafka_processed_sink (
            window_time STRING,
            type_name STRING,
            avg_temperature DOUBLE,
            median_humidity DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iot-processed-results',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """)

    # --- 9. SQL API: JOIN и отправка финального результата ---
    # Соединяем агрегированные поминутные данные со статическим справочником
    t_env.execute_sql("""
        INSERT INTO kafka_processed_sink
        SELECT 
            a.window_time,
            p.type_name,
            a.avg_temperature,
            a.median_humidity
        FROM aggregated_metrics a
        LEFT JOIN postgres_lookup p ON a.device_id = p.id
    """)

if __name__ == '__main__':
    run_job()
