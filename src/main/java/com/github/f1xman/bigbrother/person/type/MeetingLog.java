package com.github.f1xman.bigbrother.person.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.f1xman.bigbrother.TypeSerDe;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

@Value
@AllArgsConstructor(onConstructor = @__(@JsonCreator))
public class MeetingLog {

    public static final Type<MeetingLog> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.github.f1xman.bigbrother.person/MeetingLog"),
            TypeSerDe::serialize,
            bytes -> TypeSerDe.deserialize(bytes, MeetingLog.class)
    );

    @JsonProperty("persons")
    List<LoggedPerson> persons;

    public static MeetingLog createEmpty() {
        return new MeetingLog(new ArrayList<>());
    }

    public Optional<LoggedPerson> findByPersonId(String personId) {
        return findByPersonId(persons, personId);
    }

    public MeetingLog upsert(List<LoggedPerson> toUpsert) {
        Stream<LoggedPerson> personsToKeepStream = persons.stream()
                .filter(p -> findByPersonId(toUpsert, p.id).isEmpty());
        List<LoggedPerson> upserted = Stream.concat(personsToKeepStream, toUpsert.stream())
                .collect(toList());
        return new MeetingLog(upserted);
    }

    private Optional<LoggedPerson> findByPersonId(List<LoggedPerson> loggedPeople, String personId) {
        return loggedPeople
                .stream()
                .filter(p -> Objects.equals(personId, p.getId()))
                .findAny();
    }

    public MeetingLog pruneMetBefore(Before before) {
        OffsetDateTime beforeDate = before.toOffsetDateTime();
        List<LoggedPerson> personsMetAfter = persons.stream()
                .filter(p -> p.metAt.isAfter(beforeDate))
                .collect(toList());
        return new MeetingLog(personsMetAfter);
    }

    @Value
    @AllArgsConstructor(onConstructor = @__(@JsonCreator))
    public static class LoggedPerson implements Comparable<LoggedPerson> {

        @JsonProperty("id")
        String id;
        @JsonProperty("metAt")
        OffsetDateTime metAt;

        @Override
        public int compareTo(LoggedPerson that) {
            return metAt.compareTo(that.metAt);
        }
    }

    @RequiredArgsConstructor(access = PRIVATE)
    public static class Before {

        private final Clock clock;
        private final Period period;

        public static Before period(Period period, Clock clock) {
            return new Before(clock, period);
        }

        public OffsetDateTime toOffsetDateTime() {
            OffsetDateTime now = OffsetDateTime.now(clock);
            return now.minus(period);
        }
    }
}
