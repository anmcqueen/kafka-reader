package ru.mcqueen.kafka.kafka_reader.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Weather {
    private String region;
    private String temperature;
    private String wind;
    private String rainfall;
    private String humidity;

}
