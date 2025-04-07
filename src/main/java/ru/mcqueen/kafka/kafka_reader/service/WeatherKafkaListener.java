package ru.mcqueen.kafka.kafka_reader.service;

import ru.mcqueen.kafka.kafka_reader.model.Weather;

public interface WeatherKafkaListener {
    public void processMessage(Weather weather);
    public void saveWeatherData(Weather weather);
}
