import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# Настройки подключения к Kafka (внешний порт localhost:29092)
config = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'iot-generator'
}

# Инициализация продюсера
producer = Producer(config)
TOPIC_NAME = 'iot-raw-events'

def delivery_report(err, msg):
    """ Колбэк для проверки статуса доставки сообщения """
    if err is not None:
        print(f"❌ Ошибка доставки: {err}")
    else:
        print(f"✅ Отправлено: {msg.value().decode('utf-8')} -> Partition: {msg.partition()}")

print("🚀 Генератор IoT-сообщений запущен. Нажмите Ctrl+C для остановки.")

try:
    while True:
        # Имитируем данные от одного из 4-х типов устройств
        payload = {
            "id": random.randint(1, 4),
            "event_time": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "temperature": round(random.uniform(15.0, 32.0), 2),
            "humidity": round(random.uniform(40.0, 85.0), 2)
        }
        
        # Конвертируем в JSON-строку и кодируем в байты
        message_bytes = json.dumps(payload).encode('utf-8')
        
        # Асинхронная отправка в топик
        producer.produce(
            topic=TOPIC_NAME, 
            value=message_bytes, 
            callback=delivery_report
        )
        
        # Обслуживание очереди событий (вызывает колбэки)
        producer.poll(0)
        
        # Пауза в 1 секунду по условию задачи
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Генератор остановлен пользователем.")

finally:
    # Дожидаемся отправки всех оставшихся в очереди сообщений перед выходом
    print("Очистка очереди отправки...")
    producer.flush()
