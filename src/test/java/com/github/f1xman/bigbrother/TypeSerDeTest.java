package com.github.f1xman.bigbrother;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.*;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class TypeSerDeTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void writesValueAsBytes() throws Throwable {
        DumbBag testedValue = new DumbBag("value", OffsetDateTime.now());
        byte[] expectedBytes = mapper.writeValueAsBytes(testedValue);

        byte[] actualBytes = TypeSerDe.serialize(testedValue);

        assertThat(actualBytes, equalTo(expectedBytes));
    }

    @Test
    void readsValueToType() throws JsonProcessingException {
        DumbBag expectedValue = new DumbBag("value", OffsetDateTime.now());
        byte[] bytes = mapper.writeValueAsBytes(expectedValue);

        DumbBag actualValue = TypeSerDe.deserialize(bytes, DumbBag.class);

        assertThat(actualValue, equalTo(expectedValue));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    private static class DumbBag {
        private String value;
        private OffsetDateTime offsetDateTime;
    }
}