package com.github.f1xman.bigbrother.spring;

import lombok.RequiredArgsConstructor;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/")
@RequiredArgsConstructor
public class StatefunController {

    private final RequestReplyHandler handler;

    @PostMapping("/{functionName}")
    public CompletableFuture<byte[]> handle(@RequestBody byte[] body) {
        return handler
                .handle(Slices.wrap(body))
                .thenApply(Slice::toByteArray);
    }
}
