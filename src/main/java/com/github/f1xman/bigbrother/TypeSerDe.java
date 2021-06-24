package com.github.f1xman.bigbrother;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TypeSerDe {

    private static final ObjectMapper INSTANCE = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @SneakyThrows
    public static <T> byte[] serialize(T toSerialize) {
        return INSTANCE.writeValueAsBytes(toSerialize);
    }

    @SneakyThrows
    public static <T> T deserialize(byte[] toDeserialize, Class<T> type) {
        return INSTANCE.readValue(toDeserialize, type);
    }
}
