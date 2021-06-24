package com.github.f1xman.bigbrother.sector;

import com.github.f1xman.bigbrother.person.PersonFn;
import com.github.f1xman.bigbrother.person.type.SavePersonsCommand;
import com.github.f1xman.bigbrother.sector.type.EnterSectorCommand;
import com.github.f1xman.bigbrother.sector.type.QuitSectorCommand;
import com.github.f1xman.bigbrother.sector.type.SectorVisitors;
import com.github.f1xman.bigbrother.spring.StatefulFunctionSpecFactory;
import com.github.f1xman.bigbrother.spring.Statefun;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

@Statefun
@Slf4j
public class SectorFn implements StatefulFunction, StatefulFunctionSpecFactory {

    public static final TypeName TYPE = TypeName.typeNameFromString("com.github.f1xman.bigbrother/SectorFn");
    static final ValueSpec<SectorVisitors> VISITORS = ValueSpec.named("visitors").withCustomType(SectorVisitors.TYPE);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        if (message.is(EnterSectorCommand.TYPE)) {
            onEnterSector(context, message.as(EnterSectorCommand.TYPE));
        }
        if (message.is(QuitSectorCommand.TYPE)) {
            onQuitSector(context, message.as(QuitSectorCommand.TYPE));
        }
        return context.done();
    }

    private void onQuitSector(Context context, QuitSectorCommand quitSectorCommand) {
        AddressScopedStorage storage = context.storage();
        SectorVisitors sectorVisitors = storage.get(VISITORS).orElseGet(SectorVisitors::createEmpty);
        String quitVisitor = quitSectorCommand.getUserId();
        SectorVisitors updatedSectorVisitors = sectorVisitors.removeVisitor(quitVisitor);
        storage.set(VISITORS, updatedSectorVisitors);
        log.info("Person {} quit from sector {}", quitVisitor, context.self().id());
    }

    private void onEnterSector(Context context, EnterSectorCommand enterSectorCommand) {
        AddressScopedStorage storage = context.storage();
        SectorVisitors sectorVisitors = storage.get(VISITORS).orElseGet(SectorVisitors::createEmpty);
        String enteredVisitor = enterSectorCommand.getUserId();
        if (sectorVisitors.hasVisitors()) {
            Stream<Message> messagesToCurrentVisitorsStream = sectorVisitors.getVisitors()
                    .stream()
                    .map(v -> MessageBuilder.forAddress(new Address(PersonFn.TYPE, v))
                            .withCustomType(
                                    SavePersonsCommand.TYPE,
                                    new SavePersonsCommand(List.of(new SavePersonsCommand.Person(
                                            enteredVisitor,
                                            enterSectorCommand.getEnteredAt()))))
                            .build()
                    );
            List<SavePersonsCommand.Person> persons = sectorVisitors.getVisitors()
                    .stream()
                    .map(v -> new SavePersonsCommand.Person(v, enterSectorCommand.getEnteredAt()))
                    .collect(toList());
            Message messageToNewVisitor = MessageBuilder.forAddress(PersonFn.TYPE, enteredVisitor)
                    .withCustomType(SavePersonsCommand.TYPE, new SavePersonsCommand(persons))
                    .build();
            Stream.concat(Stream.of(messageToNewVisitor), messagesToCurrentVisitorsStream)
                    .forEach(context::send);
        }
        SectorVisitors updatedVisitors = sectorVisitors.addVisitor(enteredVisitor);
        storage.set(VISITORS, updatedVisitors);
        log.info("Person {} entered sector {}", enteredVisitor, context.self().id());
    }

    @Override
    public StatefulFunctionSpec createSpec(Supplier<? extends StatefulFunction> supplier) {
        return StatefulFunctionSpec.builder(TYPE)
                .withValueSpec(VISITORS)
                .withSupplier(supplier)
                .build();
    }
}
