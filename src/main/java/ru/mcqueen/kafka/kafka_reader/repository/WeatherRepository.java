package ru.mcqueen.kafka.kafka_reader.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.mcqueen.kafka.kafka_reader.model.Weather;

@Repository
public interface WeatherRepository extends JpaRepository<Weather, Long> {
}
