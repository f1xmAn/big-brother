package com.github.f1xman.bigbrother;

import com.github.f1xman.bigbrother.spring.StatefulFunctionSpecFactory;
import com.github.f1xman.bigbrother.spring.Statefun;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.context.WebApplicationContext;

import java.time.Clock;
import java.util.Map;

@SpringBootApplication
@EnableConfigurationProperties
public class BigBrotherApplication {

    public static void main(String[] args) {
        SpringApplication.run(BigBrotherApplication.class, args);
    }

    @Bean
    RequestReplyHandler requestReplyHandler(WebApplicationContext context) {
        StatefulFunctions statefulFunctions = new StatefulFunctions();
        Map<String, Object> beansWithAnnotation = context.getBeansWithAnnotation(Statefun.class);
        beansWithAnnotation.forEach((name, bean) -> {
            if (!(bean instanceof StatefulFunctionSpecFactory)) {
                throw new IllegalStateException(String.format("Function %s does not implement StatefulFunctionSpecFactory", name));
            }
            StatefulFunctionSpecFactory factory = (StatefulFunctionSpecFactory) bean;
            StatefulFunctionSpec spec = factory.createSpec(() -> context.getBean(name, StatefulFunction.class));
            statefulFunctions.withStatefulFunction(spec);
        });
        return statefulFunctions.requestReplyHandler();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
