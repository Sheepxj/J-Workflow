package com.iflytek.astron.workflow.engine;

import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class LegacyFixtureSupport {
    private LegacyFixtureSupport() {
    }

    static WorkflowDSL load(String name) throws IOException {
        return JSON.parseObject(json(name), WorkflowDSL.class);
    }

    static String json(String name) throws IOException {
        String resource = "/workflows/legacy/" + name + ".json";
        try (InputStream input = Objects.requireNonNull(LegacyFixtureSupport.class.getResourceAsStream(resource), resource)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
