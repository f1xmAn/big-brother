package com.github.f1xman.bigbrother.person.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.github.f1xman.bigbrother.TypeSerDe;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.time.OffsetDateTime;

@Value
@AllArgsConstructor(onConstructor = @__(@JsonCreator))
public class PruneMeetingLogCommand {

    public static final Type<PruneMeetingLogCommand> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.github.f1xman.bigbrother.person/PruneMeetingLogCommand"),
            TypeSerDe::serialize,
            bytes -> TypeSerDe.deserialize(bytes, PruneMeetingLogCommand.class)
    );

    OffsetDateTime sentAt;
}
