package com.github.f1xman.bigbrother.person.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.f1xman.bigbrother.TypeSerDe;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.time.OffsetDateTime;
import java.util.List;

@Value
@AllArgsConstructor(onConstructor = @__(@JsonCreator))
public class SavePersonsCommand {

    public static final Type<SavePersonsCommand> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.github.f1xman.bigbrother.person/SavePersons"),
            TypeSerDe::serialize,
            bytes -> TypeSerDe.deserialize(bytes, SavePersonsCommand.class)
    );

    @JsonProperty("persons")
    List<Person> persons;

    @Value
    @AllArgsConstructor(onConstructor = @__(@JsonCreator))
    public static class Person {

        @JsonProperty("id")
        String id;
        @JsonProperty("metAt")
        OffsetDateTime metAt;
    }
}
