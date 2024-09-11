package com.piraxx.sharder.sharderPackage;

import org.springframework.stereotype.Component;

@Component
public class ShardingContextHolder {
    private static final ThreadLocal<String> contextHolder = new ThreadLocal<>();

    public static void setCurrentShardKey(String shardKey) {
        contextHolder.set(shardKey);
    }

    public static String getCurrentShardKey() {
        return contextHolder.get();
    }

    public static void clear() {
        contextHolder.remove();
    }
}
