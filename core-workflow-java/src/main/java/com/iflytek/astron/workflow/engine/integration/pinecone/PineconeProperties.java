package com.iflytek.astron.workflow.engine.integration.pinecone;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "pinecone")
public class PineconeProperties {
    private boolean enabled;
    private String apiKey;
    private String indexHost;
    private String apiVersion = "2025-10";
    private String defaultNamespace = "default";
    private int connectTimeoutMs = 3000;
    private int readTimeoutMs = 10000;
}
