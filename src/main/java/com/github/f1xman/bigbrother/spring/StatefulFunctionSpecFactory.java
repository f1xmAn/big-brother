package com.github.f1xman.bigbrother.spring;

import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;

import java.util.function.Supplier;

public interface StatefulFunctionSpecFactory {

    StatefulFunctionSpec createSpec(Supplier<? extends StatefulFunction> supplier);
}
