package com.github.f1xman.bigbrother.sector.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.f1xman.bigbrother.TypeSerDe;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.time.OffsetDateTime;

@Value
@AllArgsConstructor(onConstructor = @__(@JsonCreator))
public class EnterSectorCommand {

    public static final Type<EnterSectorCommand> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.github.f1xman.bigbrother.sector/EnterSector"),
            TypeSerDe::serialize,
            bytes -> TypeSerDe.deserialize(bytes, EnterSectorCommand.class)
    );

    @JsonProperty("userId")
    String userId;
    @JsonProperty("enteredAt")
    OffsetDateTime enteredAt;

}
