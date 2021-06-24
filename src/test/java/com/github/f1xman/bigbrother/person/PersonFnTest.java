package com.github.f1xman.bigbrother.person;

import com.github.f1xman.bigbrother.PersonLocatedEvents;
import com.github.f1xman.bigbrother.Persons;
import com.github.f1xman.bigbrother.Sectors;
import com.github.f1xman.bigbrother.person.type.*;
import com.github.f1xman.bigbrother.sector.SectorFn;
import com.github.f1xman.bigbrother.sector.type.EnterSectorCommand;
import com.github.f1xman.bigbrother.sector.type.QuitSectorCommand;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.*;
import java.util.List;
import java.util.Optional;

import static com.github.f1xman.bigbrother.MessageMatchers.messageEq;
import static com.github.f1xman.bigbrother.MessageMatchers.typeIs;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PersonFnTest {

    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());
    private static final String PERSON_MIGHT_BE_INFECTED_EVENT_TOPIC = "topic";
    private static final Period PRUNE_MEETING_LOG_AFTER_PERIOD = Period.ofWeeks(2);
    private final PersonFn personFn = new PersonFn(PERSON_MIGHT_BE_INFECTED_EVENT_TOPIC, PRUNE_MEETING_LOG_AFTER_PERIOD, CLOCK);
    @Mock
    private Context context;
    @Mock
    private AddressScopedStorage storage;

    @Test
    void savesNewSectorId() {
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.empty()).when(storage).get(PersonFn.SECTOR_ID);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, "alice")
                .withCustomType(
                        LocatePersonCommand.TYPE,
                        PersonLocatedEvents.aliceLocatedInSectorBlue(CLOCK)
                )
                .build());

        verify(storage).set(PersonFn.SECTOR_ID, "blue");
    }

    @Test
    void sendsEnterSectorIfNewSectorIdReceived() {
        Message expectedMessage = MessageBuilder
                .forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(
                        EnterSectorCommand.TYPE, new EnterSectorCommand(Persons.ALICE, OffsetDateTime.now(CLOCK))
                )
                .build();
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.empty()).when(storage).get(PersonFn.SECTOR_ID);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(
                        LocatePersonCommand.TYPE,
                        PersonLocatedEvents.aliceLocatedInSectorBlue(CLOCK)
                )
                .build());

        verify(context).send(messageEq(expectedMessage));
    }

    @Test
    void sendsQuitSectorIfNewSectorIdReceived() {
        Message expectedMessage = MessageBuilder.forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(QuitSectorCommand.TYPE, new QuitSectorCommand(Persons.ALICE, OffsetDateTime.now(CLOCK)))
                .build();
        doReturn(storage).when(context).storage();
        doReturn(Optional.of(Sectors.BLUE)).when(storage).get(PersonFn.SECTOR_ID);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(
                        LocatePersonCommand.TYPE,
                        PersonLocatedEvents.aliceLocatedInSectorGreen(CLOCK)
                )
                .build());

        verify(context).send(messageEq(expectedMessage));
    }

    @Test
    void doesNotSendEnterSectorIfSectorIsTheSame() {
        doReturn(storage).when(context).storage();
        doReturn(Optional.of(Sectors.BLUE)).when(storage).get(PersonFn.SECTOR_ID);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(
                        LocatePersonCommand.TYPE,
                        PersonLocatedEvents.aliceLocatedInSectorBlue(CLOCK)
                )
                .build());

        verify(context, never()).send(typeIs(EnterSectorCommand.TYPE));
    }

    @Test
    void sendsAlertInfectionRiskIfPersonInfected() {
        Message expectedMessage = MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.BOB)
                .withCustomType(
                        AlertInfectionRiskCommand.TYPE,
                        new AlertInfectionRiskCommand(Persons.ALICE, OffsetDateTime.now(CLOCK))
                )
                .build();
        MeetingLog meetingLog = new MeetingLog(List.of(new MeetingLog.LoggedPerson(Persons.BOB, OffsetDateTime.now(CLOCK))));
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.of(meetingLog)).when(storage).get(PersonFn.MEETING_LOG);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(
                        AlertInfectionCommand.TYPE,
                        new AlertInfectionCommand(Persons.ALICE, OffsetDateTime.now(CLOCK))
                )
                .build());

        verify(context).send(messageEq(expectedMessage));
    }

    @Test
    void doesNotSendAlertInfectionRiskOnAlertInfectionIfPersonsLogDoesNotExistYet() {
        Message message = MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(
                        AlertInfectionCommand.TYPE,
                        new AlertInfectionCommand(Persons.ALICE, OffsetDateTime.now(CLOCK))
                )
                .build();
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.empty()).when(storage).get(PersonFn.MEETING_LOG);

        personFn.apply(context, message);

        verify(context, never()).send(typeIs(AlertInfectionRiskCommand.TYPE));
    }

    @Test
    void addsNewPersonToPersonsLog() {
        List<MeetingLog.LoggedPerson> expectedPersons = List.of(
                new MeetingLog.LoggedPerson(Persons.JOE, OffsetDateTime.now(CLOCK)),
                new MeetingLog.LoggedPerson(Persons.BOB, OffsetDateTime.now(CLOCK))
        );
        MeetingLog lastMeetingLog = new MeetingLog(List.of(new MeetingLog.LoggedPerson(Persons.JOE, OffsetDateTime.now(CLOCK))));
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.of(lastMeetingLog)).when(storage).get(PersonFn.MEETING_LOG);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(
                        SavePersonsCommand.TYPE,
                        new SavePersonsCommand(List.of(new SavePersonsCommand.Person(Persons.BOB, OffsetDateTime.now(CLOCK))))
                )
                .build());

        verify(storage).set(eq(PersonFn.MEETING_LOG), personsEqualIgnoreOrder(expectedPersons));
    }

    @Test
    void addsFirstPersonToPersonsLog() {
        List<MeetingLog.LoggedPerson> expectedPersons = List.of(
                new MeetingLog.LoggedPerson(Persons.BOB, OffsetDateTime.now(CLOCK))
        );
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.empty()).when(storage).get(PersonFn.MEETING_LOG);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(
                        SavePersonsCommand.TYPE,
                        new SavePersonsCommand(List.of(new SavePersonsCommand.Person(Persons.BOB, OffsetDateTime.now(CLOCK))))
                )
                .build());

        verify(storage).set(eq(PersonFn.MEETING_LOG), personsEqualIgnoreOrder(expectedPersons));

    }

    private MeetingLog personsEqualIgnoreOrder(List<MeetingLog.LoggedPerson> expectedPersons) {
        return argThat(l -> l.getPersons().containsAll(expectedPersons) && l.getPersons().size() == expectedPersons.size());
    }

    @Test
    void sendsPersonMightBeInfectedEventIfAlertInfectionRiskReceived() {
        EgressMessage expectedMessage = KafkaEgressMessage.forEgress(
                TypeName.typeNameOf(
                        "com.github.f1xman.bigbrother",
                        "PersonMightBeInfectedEvent"
                ))
                .withTopic("topic")
                .withUtf8Key(Persons.ALICE)
                .withValue(PersonMightBeInfectedEvent.TYPE, new PersonMightBeInfectedEvent(
                                Persons.ALICE,
                                OffsetDateTime.now(CLOCK)
                        )
                )
                .build();
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();

        this.personFn.apply(context, MessageBuilder.forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(AlertInfectionRiskCommand.TYPE, new AlertInfectionRiskCommand(
                        Persons.BOB,
                        OffsetDateTime.now(CLOCK)
                ))
                .build()
        );

        verify(context).send(messageEq(expectedMessage));
    }

    @Test
    void removesOutdatedLoggedPersonOnPruneMeetingLog() {
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        OffsetDateTime twoWeeksAgo = OffsetDateTime.now(CLOCK).minusWeeks(2);
        MeetingLog.LoggedPerson loggedPerson = new MeetingLog.LoggedPerson(Persons.BOB, twoWeeksAgo);
        MeetingLog meetingLog = new MeetingLog(List.of(loggedPerson));
        doReturn(Optional.of(meetingLog)).when(storage).get(PersonFn.MEETING_LOG);
        MeetingLog expectedMeetingLog = MeetingLog.createEmpty();

        personFn.apply(context, MessageBuilder.forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(PruneMeetingLogCommand.TYPE, new PruneMeetingLogCommand(OffsetDateTime.now(CLOCK)))
                .build()
        );

        verify(storage).set(PersonFn.MEETING_LOG, expectedMeetingLog);
    }

    @Test
    void setsEmptyMeetingLogOnPruneMeetingLogIfLogDoesNotPresentYet() {
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.empty()).when(storage).get(PersonFn.MEETING_LOG);
        MeetingLog expectedMeetingLog = MeetingLog.createEmpty();

        personFn.apply(context, MessageBuilder.forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(PruneMeetingLogCommand.TYPE, new PruneMeetingLogCommand(OffsetDateTime.now(CLOCK)))
                .build()
        );

        verify(storage).set(PersonFn.MEETING_LOG, expectedMeetingLog);
    }

    @Test
    void schedulesPruneMeetingLogOnPruneMeetingLog() {
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.empty()).when(storage).get(PersonFn.MEETING_LOG);

        personFn.apply(context, MessageBuilder.forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(PruneMeetingLogCommand.TYPE, new PruneMeetingLogCommand(OffsetDateTime.now(CLOCK)))
                .build()
        );

        verify(context).sendAfter(eq(Duration.ofDays(1)), messageEq(MessageBuilder.forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(PruneMeetingLogCommand.TYPE, new PruneMeetingLogCommand(OffsetDateTime.now(CLOCK)))
                .build())
        );
    }

    @Test
    void schedulesPruneMeetingLogOnFirstLocatePerson() {
        doReturn(new Address(PersonFn.TYPE, Persons.ALICE)).when(context).self();
        doReturn(storage).when(context).storage();
        doReturn(Optional.empty()).when(storage).get(PersonFn.SECTOR_ID);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, "alice")
                .withCustomType(
                        LocatePersonCommand.TYPE,
                        PersonLocatedEvents.aliceLocatedInSectorBlue(CLOCK)
                )
                .build());

        verify(context).sendAfter(eq(Duration.ofDays(1)), messageEq(MessageBuilder.forAddress(PersonFn.TYPE, Persons.ALICE)
                .withCustomType(PruneMeetingLogCommand.TYPE, new PruneMeetingLogCommand(OffsetDateTime.now(CLOCK)))
                .build())
        );
    }

    @Test
    void doesNotSchedulePruneMeetingLogOnEachLocatePerson() {
        doReturn(storage).when(context).storage();
        doReturn(Optional.of(Sectors.BLUE)).when(storage).get(PersonFn.SECTOR_ID);

        personFn.apply(context, MessageBuilder
                .forAddress(PersonFn.TYPE, "alice")
                .withCustomType(
                        LocatePersonCommand.TYPE,
                        PersonLocatedEvents.aliceLocatedInSectorBlue(CLOCK)
                )
                .build());

        verify(context, never()).sendAfter(eq(Duration.ofDays(1)), typeIs(PruneMeetingLogCommand.TYPE));
    }
}