package com.github.f1xman.bigbrother.sector;

import com.github.f1xman.bigbrother.Persons;
import com.github.f1xman.bigbrother.Sectors;
import com.github.f1xman.bigbrother.person.PersonFn;
import com.github.f1xman.bigbrother.person.type.SavePersonsCommand;
import com.github.f1xman.bigbrother.sector.type.EnterSectorCommand;
import com.github.f1xman.bigbrother.sector.type.QuitSectorCommand;
import com.github.f1xman.bigbrother.sector.type.SectorVisitors;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static com.github.f1xman.bigbrother.MessageMatchers.messageEq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SectorFnTest {

    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());
    private final SectorFn sectorFn = new SectorFn();
    @Mock
    private Context context;
    @Mock
    private AddressScopedStorage storage;

    @BeforeEach
    void setUp() {
        doReturn(storage).when(context).storage();
        doReturn(new Address(SectorFn.TYPE, Sectors.BLUE)).when(context).self();
    }

    @Test
    void addsPersonToSectorVisitorsOnEnterSector() {
        SectorVisitors expectedVisitors = new SectorVisitors(List.of(Persons.ALICE));
        doReturn(Optional.empty()).when(storage).get(SectorFn.VISITORS);

        sectorFn.apply(context, MessageBuilder.forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(EnterSectorCommand.TYPE, new EnterSectorCommand(Persons.ALICE, OffsetDateTime.now(CLOCK)))
                .build()
        );

        verify(storage).set(SectorFn.VISITORS, expectedVisitors);
    }

    @Test
    void sendsSavePersonsWithNewVisitorToCurrentVisitors() {
        doReturn(Optional.of(new SectorVisitors(List.of(Persons.BOB)))).when(storage).get(SectorFn.VISITORS);
        OffsetDateTime sectorEnteredAt = OffsetDateTime.now(CLOCK);

        sectorFn.apply(context, MessageBuilder.forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(EnterSectorCommand.TYPE, new EnterSectorCommand(Persons.ALICE, sectorEnteredAt))
                .build()
        );

        verify(context).send(messageEq(
                MessageBuilder.forAddress(PersonFn.TYPE, Persons.BOB)
                        .withCustomType(
                                SavePersonsCommand.TYPE,
                                new SavePersonsCommand(List.of(new SavePersonsCommand.Person(Persons.ALICE, sectorEnteredAt))))
                        .build())
        );
    }

    @Test
    void sendsSavePersonsWithCurrentVisitorsToNewVisitor() {
        doReturn(Optional.of(new SectorVisitors(List.of(Persons.BOB)))).when(storage).get(SectorFn.VISITORS);
        OffsetDateTime sectorEnteredAt = OffsetDateTime.now(CLOCK);

        sectorFn.apply(context, MessageBuilder.forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(EnterSectorCommand.TYPE, new EnterSectorCommand(Persons.ALICE, sectorEnteredAt))
                .build()
        );

        verify(context).send(messageEq(
                MessageBuilder.forAddress(PersonFn.TYPE, Persons.ALICE)
                        .withCustomType(SavePersonsCommand.TYPE, new SavePersonsCommand(
                                List.of(new SavePersonsCommand.Person(Persons.BOB, sectorEnteredAt))
                        ))
                        .build()
        ));
    }

    @Test
    void removesVisitorOnQuitSector() {
        SectorVisitors expectedVisitors = SectorVisitors.createEmpty();
        doReturn(Optional.of(new SectorVisitors(List.of(Persons.BOB)))).when(storage).get(SectorFn.VISITORS);
        OffsetDateTime sectorQuitAt = OffsetDateTime.now(CLOCK);

        sectorFn.apply(context, MessageBuilder.forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(QuitSectorCommand.TYPE, new QuitSectorCommand(Persons.BOB, sectorQuitAt))
                .build()
        );

        verify(storage).set(SectorFn.VISITORS, expectedVisitors);
    }

    @Test
    void savesEmptySectorVisitorsIfSectorVisitorsDoesNotExistOnQuitSector() {
        SectorVisitors expectedVisitors = SectorVisitors.createEmpty();
        doReturn(Optional.empty()).when(storage).get(SectorFn.VISITORS);

        sectorFn.apply(context, MessageBuilder.forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(QuitSectorCommand.TYPE, new QuitSectorCommand(Persons.BOB, OffsetDateTime.now(CLOCK)))
                .build()
        );

        verify(storage).set(SectorFn.VISITORS, expectedVisitors);
    }

    @Test
    void doesNotSendAnythingIfThereIsNoCurrentVisitors() {
        doReturn(Optional.of(SectorVisitors.createEmpty())).when(storage).get(SectorFn.VISITORS);
        OffsetDateTime sectorEnteredAt = OffsetDateTime.now(CLOCK);

        sectorFn.apply(context, MessageBuilder.forAddress(SectorFn.TYPE, Sectors.BLUE)
                .withCustomType(EnterSectorCommand.TYPE, new EnterSectorCommand(Persons.ALICE, sectorEnteredAt))
                .build()
        );

        verify(context, never()).send(any(Message.class));
    }
}