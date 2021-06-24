package com.github.f1xman.bigbrother.person;

import com.github.f1xman.bigbrother.person.type.*;
import com.github.f1xman.bigbrother.sector.SectorFn;
import com.github.f1xman.bigbrother.sector.type.EnterSectorCommand;
import com.github.f1xman.bigbrother.sector.type.QuitSectorCommand;
import com.github.f1xman.bigbrother.spring.StatefulFunctionSpecFactory;
import com.github.f1xman.bigbrother.spring.Statefun;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.springframework.beans.factory.annotation.Value;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.Period;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.github.f1xman.bigbrother.person.type.MeetingLog.Before;
import static com.github.f1xman.bigbrother.person.type.MeetingLog.LoggedPerson;
import static java.util.stream.Collectors.joining;

@Slf4j
@RequiredArgsConstructor
@Statefun
public class PersonFn implements StatefulFunction, StatefulFunctionSpecFactory {

    public static final TypeName TYPE = TypeName.typeNameFromString("com.github.f1xman.bigbrother/PersonFn");
    static final ValueSpec<String> SECTOR_ID = ValueSpec.named("sectorId").withUtf8StringType();
    static final ValueSpec<MeetingLog> MEETING_LOG = ValueSpec.named("meetingLog").withCustomType(MeetingLog.TYPE);
    @Value("${egresses.person-might-be-infected-event.topic}")
    private final String personMightBeInfectedEventTopic;
    @Value("${persons.prune-meeting-log-after}")
    private final Period pruneMeetingLogAfterPeriod;
    private final Clock clock;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        if (message.is(LocatePersonCommand.TYPE)) {
            onLocatePerson(context, message.as(LocatePersonCommand.TYPE));
        }
        if (message.is(AlertInfectionCommand.TYPE)) {
            onAlertInfection(context, message.as(AlertInfectionCommand.TYPE));
        }
        if (message.is(SavePersonsCommand.TYPE)) {
            onSavePersons(context, message.as(SavePersonsCommand.TYPE));
        }
        if (message.is(AlertInfectionRiskCommand.TYPE)) {
            onAlertInfectionRisk(context, message.as(AlertInfectionRiskCommand.TYPE));
        }
        if (message.is(PruneMeetingLogCommand.TYPE)) {
            onPruneMeetingLog(context);
        }
        return context.done();
    }

    private void onPruneMeetingLog(Context context) {
        AddressScopedStorage storage = context.storage();
        MeetingLog meetingLog = storage.get(MEETING_LOG).orElseGet(MeetingLog::createEmpty);
        MeetingLog updatedMeetingLog = meetingLog.pruneMetBefore(Before.period(pruneMeetingLogAfterPeriod, clock));
        storage.set(MEETING_LOG, updatedMeetingLog);
        schedulePruneMeetingLogCommand(context);
    }

    private void onAlertInfectionRisk(Context context, AlertInfectionRiskCommand alertInfectionRiskCommand) {
        String infectedPersonId = alertInfectionRiskCommand.getInfectedPersonId();
        String selfId = context.self().id();
        log.info(
                "Person {} might be infected due to recent contact with {}, emitting PersonMightBeInfectedEvent...",
                selfId, infectedPersonId
        );
        context.send(
                KafkaEgressMessage.forEgress(
                        TypeName.typeNameFromString("com.github.f1xman.bigbrother/PersonMightBeInfectedEvent")
                )
                        .withUtf8Key(selfId)
                        .withTopic(personMightBeInfectedEventTopic)
                        .withValue(PersonMightBeInfectedEvent.TYPE, new PersonMightBeInfectedEvent(
                                selfId, OffsetDateTime.now(clock)
                        ))
                        .build()
        );
    }

    private void onLocatePerson(Context context, LocatePersonCommand locatePersonCommand) {
        AddressScopedStorage storage = context.storage();
        Optional<String> lastSectorId = storage.get(SECTOR_ID);
        String locatedSectorId = locatePersonCommand.getSectorId();
        if (lastSectorId.isPresent()) {
            String lastSectorIdValue = lastSectorId.get();
            if (!lastSectorIdValue.equals(locatedSectorId)) {
                enterSector(context, locatePersonCommand, storage, locatedSectorId);
                quitSector(context, locatePersonCommand, lastSectorIdValue);
            }
        } else {
            log.info("Person {} located first time", context.self().id());
            schedulePruneMeetingLogCommand(context);
            enterSector(context, locatePersonCommand, storage, locatedSectorId);
        }
    }

    private void schedulePruneMeetingLogCommand(Context context) {
        log.info("Scheduling PruneMeetingLogCommand for person {}", context.self().id());
        context.sendAfter(Duration.ofDays(1), MessageBuilder.forAddress(context.self())
                .withCustomType(PruneMeetingLogCommand.TYPE, new PruneMeetingLogCommand(OffsetDateTime.now(clock)))
                .build()
        );
    }

    private void onSavePersons(Context context, SavePersonsCommand savePersonsCommand) {
        AddressScopedStorage storage = context.storage();
        MeetingLog meetingLog = storage.get(MEETING_LOG).orElseGet(MeetingLog::createEmpty);
        List<LoggedPerson> toUpsert = savePersonsCommand.getPersons()
                .stream()
                .map(p -> new MeetingLog.LoggedPerson(p.getId(), p.getMetAt()))
                .collect(Collectors.toList());
        MeetingLog meetingLogToSave = meetingLog.upsert(toUpsert);
        storage.set(MEETING_LOG, meetingLogToSave);
        log.info("Persons [{}] added to meeting log of person {}", savePersonsCommand.getPersons().stream()
                .map(SavePersonsCommand.Person::getId)
                .collect(joining(", ")), context.self().id());
    }

    private void onAlertInfection(Context context, AlertInfectionCommand alertInfectionCommand) {
        log.info("Person {} infected", context.self().id());
        AddressScopedStorage storage = context.storage();
        MeetingLog meetingLog = storage.get(MEETING_LOG).orElseGet(MeetingLog::createEmpty);
        List<LoggedPerson> persons = meetingLog.getPersons();
        String infectedPersonId = alertInfectionCommand.getPersonId();
        OffsetDateTime diagnosedAt = alertInfectionCommand.getDiagnosedAt();
        AlertInfectionRiskCommand alertInfectionRiskCommand = new AlertInfectionRiskCommand(infectedPersonId, diagnosedAt);
        persons.stream()
                .map(LoggedPerson::getId)
                .forEach(pId -> {
                    Message alertInfectionRiskMessage = MessageBuilder.forAddress(new Address(PersonFn.TYPE, pId))
                            .withCustomType(AlertInfectionRiskCommand.TYPE, alertInfectionRiskCommand)
                            .build();
                    context.send(alertInfectionRiskMessage);
                    log.info("AlertInfectionRiskCommand sent to person {}", pId);
                });
    }

    private void quitSector(Context context, LocatePersonCommand locatePersonCommand, String lastSectorIdValue) {
        Message quitSectorMessage = MessageBuilder
                .forAddress(SectorFn.TYPE, lastSectorIdValue)
                .withCustomType(QuitSectorCommand.TYPE, new QuitSectorCommand(
                        locatePersonCommand.getPersonId(),
                        locatePersonCommand.getLocatedAt()
                ))
                .build();
        context.send(quitSectorMessage);
    }

    private void enterSector(Context context, LocatePersonCommand locatePersonCommand, AddressScopedStorage storage, String locatedSectorId) {
        storage.set(SECTOR_ID, locatedSectorId);
        Message enterSectorMessage = MessageBuilder
                .forAddress(SectorFn.TYPE, locatedSectorId)
                .withCustomType(EnterSectorCommand.TYPE, new EnterSectorCommand(
                        locatePersonCommand.getPersonId(),
                        locatePersonCommand.getLocatedAt()
                ))
                .build();
        context.send(enterSectorMessage);
    }

    @Override
    public StatefulFunctionSpec createSpec(Supplier<? extends StatefulFunction> supplier) {
        return StatefulFunctionSpec.builder(PersonFn.TYPE)
                .withValueSpecs(SECTOR_ID, MEETING_LOG)
                .withSupplier(supplier)
                .build();
    }
}
