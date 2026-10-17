package com.iflytek.astron.console.toolkit.knowledge;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "knowledge.local-processing")
public class LocalDocumentProcessingProperties {
    private boolean enabled = true;
    private boolean fallbackToExternal;
    private int targetSize = 700;
    private int minSize = 100;
    private int maxSize = 1000;
    private int overlapSize = 100;
}
