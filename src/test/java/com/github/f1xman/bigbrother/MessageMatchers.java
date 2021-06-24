package com.github.f1xman.bigbrother;

import lombok.NoArgsConstructor;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.types.Type;

import static lombok.AccessLevel.PRIVATE;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.refEq;

@NoArgsConstructor(access = PRIVATE)
public class MessageMatchers {

    public static <T> Message typeIs(Type<T> type) {
        return argThat(argument -> argument.is(type));
    }

    public static Message messageEq(Message message) {
        return refEq(message);
    }

    public static EgressMessage messageEq(EgressMessage message) {
        return refEq(message);
    }
}
