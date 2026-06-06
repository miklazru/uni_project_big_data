package ru.hse.bigdata;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public class IoTEvent {
    public int id;
    
    @JsonProperty("event_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    public String event_time;
    
    public double temperature;
    public double humidity;

    public IoTEvent() {} // Пустой конструктор нужен для Flink POJO
}
