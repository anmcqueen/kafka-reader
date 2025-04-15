package ru.mcqueen.kafka.kafka_reader.utils;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.IntputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class FileReader {
    public static String fileAsString(String recourceName) throws IOException {
        try (IntputStream in = FileReader.class.getClassLoader().getResourceAsStream(recourceName)) {
            return IOUtils.toString(Objects.requireNonNull(in), Charsets.toCharset(StandardCharsets.UTF_8));
        }
    }
}
