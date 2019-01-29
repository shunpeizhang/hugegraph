package com.baidu.hugegraph.backend.store.ultrasearch;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

public class UltraSearchOptions extends OptionHolder {

    private UltraSearchOptions() {
        super();
    }

    private static volatile UltraSearchOptions instance;

    public static synchronized UltraSearchOptions instance() {
        if (instance == null) {
            instance = new UltraSearchOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> ULTRASEARCH_IP =
            new ConfigOption<>(
                    "ultrasearch.ip",
                    "The ip of database in ultrasearch format.",
                    disallowEmpty(),
                    "172.16.11.17"
            );


}
