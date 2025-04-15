package ru.mcqueen.kafka.kafka_reader;

import org.apache.kafka.common.protocol.types.Field;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import ru.mcqueen.kafka.kafka_reader.model.Weather;
import ru.mcqueen.kafka.kafka_reader.repository.WeatherRepository;
import ru.mcqueen.kafka.kafka_reader.utils.FileReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
		brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" },
		topics = { "${app.kafka.topic.name}" }
)
public class WeatherKafkaListenerE2ETest {

	@Autowired
	private KafkaTemplate<String, Weather> kafkaTemplate;

	@Autowired
	private WeatherRepository weatherRepository;

	@Value("${app.kafka.topic.name}")
	private String topicName;

	private static final String MESSAGE_ONE = "payloads/message-one.json";

	// Очищаем БД перед каждым тестом для изоляции
	@Before
	public void setUp() {
		weatherRepository.deleteAll();
	}

	@Test
	public void ReceiveWeatherMessageAndSaveToDb() throws Exception {
		Weather testWeather = new Weather();
		testWeather.setRegion("Moscow");
		testWeather.setTemperature("+15C");
		testWeather.setWind("5 m/s NW");
		testWeather.setRainfall("8mm");
		testWeather.setHumidity("60%");

		// Отправляем сообщение в топик Kafka
		sendMessage("Moscow", FileReader.fileAsString(MESSAGE_ONE));

		// Проверки
		Thread.sleep(3000);

		// Проверяем, что в базе есть ровно одна запись
		List<Weather> savedWeatherData = weatherRepository.findAll();
		assertThat(savedWeatherData).hasSize(1);

		// Проверяем содержимое сохраненной записи
		Weather saved = savedWeatherData.get(0);
		assertNotNull(saved.getId());
		assertEquals(saved.getRegion(), testWeather.getRegion());
		assertEquals(saved.getTemperature(), testWeather.getTemperature());
		assertEquals(saved.getWind(), testWeather.getWind());
		assertEquals(saved.getRainfall(), testWeather.getRainfall());
		assertEquals(saved.getHumidity(), testWeather.getHumidity());
	}

	private void sendMessage(String key, String message) {
		kafkaTemplate.send(new GenericMessage<>(message, getHeaders(topicName, key)));
		kafkaTemplate.flush(); // Гарантируем отправку

	}

	private Map<String, Serializable> getHeaders(String topic, String key) {
		return Map.of(
				KafkaHeaders.TOPIC, topic,
				KafkaHeaders.KEY, key
		);
	}

}
