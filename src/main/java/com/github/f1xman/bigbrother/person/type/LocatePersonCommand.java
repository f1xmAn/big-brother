package com.github.f1xman.bigbrother.person.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.f1xman.bigbrother.TypeSerDe;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.time.OffsetDateTime;

@Value
@AllArgsConstructor(onConstructor = @__(@JsonCreator))
public class LocatePersonCommand {

    public static final Type<LocatePersonCommand> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.github.f1xman.bigbrother.person/LocatePerson"),
            TypeSerDe::serialize,
            bytes -> TypeSerDe.deserialize(bytes, LocatePersonCommand.class)
    );

    @JsonProperty("sectorId")
    String sectorId;
    @JsonProperty("personId")
    String personId;
    @JsonProperty("locatedAt")
    OffsetDateTime locatedAt;
}
