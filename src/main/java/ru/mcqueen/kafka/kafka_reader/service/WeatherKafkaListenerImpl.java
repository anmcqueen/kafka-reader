package ru.mcqueen.kafka.kafka_reader.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.mcqueen.kafka.kafka_reader.model.Weather;
import ru.mcqueen.kafka.kafka_reader.repository.WeatherRepository;


@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherKafkaListenerImpl implements WeatherKafkaListener {

    private final WeatherRepository weatherRepository;

    @Override
    @KafkaListener(topics = "${app.kafka.topic.name}")
    public void processMessage(Weather weather) {
        log.info("Received Weather message: {}", weather);
        log.info("Saving Weather data to the table weather_data: {}", weather);
        saveWeatherData(weather);
    }

    public void saveWeatherData(Weather weather) {
        weatherRepository.save(weather);
    }
}
