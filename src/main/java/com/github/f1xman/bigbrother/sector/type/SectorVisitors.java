package com.github.f1xman.bigbrother.sector.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.f1xman.bigbrother.TypeSerDe;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.*;
import static java.util.stream.Collectors.toList;

@Value
@AllArgsConstructor(onConstructor = @__(@JsonCreator))
public class SectorVisitors {

    public static final Type<SectorVisitors> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.github.f1xman.bigbrother.sector/SectorVisitors"),
            TypeSerDe::serialize,
            bytes -> TypeSerDe.deserialize(bytes, SectorVisitors.class)
    );

    @JsonProperty("visitors")
    List<String> visitors;

    public static SectorVisitors createEmpty() {
        return new SectorVisitors(new ArrayList<>());
    }

    public SectorVisitors addVisitor(String personId) {
        List<String> updatedVisitors = Stream.concat(Stream.of(personId), visitors.stream())
                .collect(toList());
        return new SectorVisitors(updatedVisitors);
    }

    public SectorVisitors removeVisitor(String personId) {
        List<String> updatedVisitors = visitors.stream()
                .filter(not(personId::equals))
                .collect(toList());
        return new SectorVisitors(updatedVisitors);
    }

    public boolean hasVisitors() {
        return !visitors.isEmpty();
    }
}
