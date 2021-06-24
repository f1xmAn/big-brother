package com.github.f1xman.bigbrother.person;

import com.github.f1xman.bigbrother.Persons;
import com.github.f1xman.bigbrother.person.type.MeetingLog;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.List;
import java.util.Optional;

import static com.github.f1xman.bigbrother.person.type.MeetingLog.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeetingLogTest {

    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

    @Test
    void returnsIfMetAtEqual() {
        LoggedPerson a = new LoggedPerson(Persons.ALICE, OffsetDateTime.now(CLOCK));
        LoggedPerson b = new LoggedPerson(Persons.ALICE, OffsetDateTime.now(CLOCK));

        assertThat(a.compareTo(b), equalTo(0));
    }

    @Test
    void returnsNegativeValueIfAHasEarlierMetAt() {
        LoggedPerson a = new LoggedPerson(Persons.ALICE, OffsetDateTime.now(CLOCK).minusDays(1));
        LoggedPerson b = new LoggedPerson(Persons.ALICE, OffsetDateTime.now(CLOCK));

        assertThat(a.compareTo(b), equalTo(-1));
    }

    @Test
    void returnsPositiveValueIfAHasLaterMetAt() {
        LoggedPerson a = new LoggedPerson(Persons.ALICE, OffsetDateTime.now(CLOCK).plusDays(1));
        LoggedPerson b = new LoggedPerson(Persons.ALICE, OffsetDateTime.now(CLOCK));

        assertThat(a.compareTo(b), equalTo(1));
    }

    @Test
    void returnsLoggedPersonIfThereIsOneWithGivenId() {
        LoggedPerson expectedLoggedPerson = new LoggedPerson(Persons.ALICE, OffsetDateTime.now(CLOCK));
        MeetingLog meetingLog = new MeetingLog(List.of(expectedLoggedPerson));

        LoggedPerson actualLoggedPerson = meetingLog.findByPersonId(Persons.ALICE).get();

        assertThat(actualLoggedPerson, equalTo(expectedLoggedPerson));
    }

    @Test
    void returnsEmptyOptionalIfThereIsNoLoggedPersonWithGivenId() {
        MeetingLog meetingLog = createEmpty();

        Optional<LoggedPerson> actualLoggedPerson = meetingLog.findByPersonId(Persons.ALICE);

        assertTrue(actualLoggedPerson.isEmpty());
    }

    @Test
    void returnsNewMeetingLogContainingUpsertedLoggedPersons() {
        List<LoggedPerson> expectedPersons = List.of(
                new LoggedPerson(Persons.ALICE, OffsetDateTime.MAX),
                new LoggedPerson(Persons.BOB, OffsetDateTime.MAX),
                new LoggedPerson(Persons.JOE, OffsetDateTime.MAX)
        );
        MeetingLog meetingLog = new MeetingLog(List.of(
                new LoggedPerson(Persons.ALICE, OffsetDateTime.MIN),
                new LoggedPerson(Persons.BOB, OffsetDateTime.MAX)
        ));

        MeetingLog actualMeetingLog = meetingLog.upsert(List.of(
                new LoggedPerson(Persons.ALICE, OffsetDateTime.MAX),
                new LoggedPerson(Persons.JOE, OffsetDateTime.MAX)
        ));

        assertThat(actualMeetingLog.getPersons(), containsInAnyOrder(expectedPersons.toArray()));
    }

    @Test
    void removesLoggedPersonsMetGivenPeriodAgo() {
        OffsetDateTime twoWeeksAndDayAgo = OffsetDateTime.now(CLOCK).minusDays(15);
        LoggedPerson metBeforePerson = new LoggedPerson(Persons.ALICE, twoWeeksAndDayAgo);
        LoggedPerson metAfterPerson = new LoggedPerson(Persons.BOB, OffsetDateTime.now(CLOCK));
        MeetingLog meetingLog = new MeetingLog(List.of(metBeforePerson, metAfterPerson));
        MeetingLog expectedMeetingLog = new MeetingLog(List.of(metAfterPerson));

        MeetingLog actualMeetingLog = meetingLog.pruneMetBefore(Before.period(Period.ofWeeks(2), CLOCK));

        assertThat(actualMeetingLog, equalTo(expectedMeetingLog));
    }

    @Test
    void calculatesDateBefore() {
        Period period = Period.ofDays(1);
        OffsetDateTime expectedDate = OffsetDateTime.now(CLOCK).minus(period);
        Before before = Before.period(period, CLOCK);

        OffsetDateTime actualDate = before.toOffsetDateTime();

        assertThat(actualDate, equalTo(expectedDate));
    }
}