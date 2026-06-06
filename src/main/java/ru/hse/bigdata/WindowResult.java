package ru.hse.bigdata;

import java.time.LocalDateTime;

public class WindowResult {
    public int id;
    public LocalDateTime windowEnd;
    public double avgTemperature;
    public double medianHumidity;

    public WindowResult() {}

    public WindowResult(int id, LocalDateTime windowEnd, double avgTemperature, double medianHumidity) {
        this.id = id;
        this.windowEnd = windowEnd;
        this.avgTemperature = avgTemperature;
        this.medianHumidity = medianHumidity;
    }
}
