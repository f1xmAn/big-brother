package com.github.f1xman.bigbrother;

import com.github.f1xman.bigbrother.person.type.LocatePersonCommand;
import lombok.NoArgsConstructor;

import java.time.Clock;
import java.time.OffsetDateTime;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class PersonLocatedEvents {

    public static LocatePersonCommand aliceLocatedInSectorBlue(Clock clock) {
        return new LocatePersonCommand(Sectors.BLUE, Persons.ALICE, OffsetDateTime.now(clock));
    }

    public static LocatePersonCommand aliceLocatedInSectorGreen(Clock clock) {
        return new LocatePersonCommand(Sectors.GREEN, Persons.ALICE, OffsetDateTime.now(clock));
    }
}
