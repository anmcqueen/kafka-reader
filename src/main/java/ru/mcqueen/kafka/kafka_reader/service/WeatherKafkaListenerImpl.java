package ru.mcqueen.kafka.kafka_reader.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.mcqueen.kafka.kafka_reader.model.Weather;


@Slf4j
@Service
public class WeatherKafkaListenerImpl implements WeatherKafkaListener {

    @Override
    @KafkaListener(topics = "${app.kafka.topic.name}")
    public void processMessage(Weather weather) {
        System.out.println("Received Weather message: " + weather);
    }
}
