package ru.mcqueen.kafka.kafka_reader.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "weather_data")
@Data
@NoArgsConstructor
public class Weather {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String region;
    private String temperature;
    private String wind;
    private String rainfall;
    private String humidity;

}
