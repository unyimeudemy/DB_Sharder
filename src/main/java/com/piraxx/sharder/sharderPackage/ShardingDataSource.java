package com.piraxx.sharder.sharderPackage;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class ShardingDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return ShardingContextHolder.getCurrentShardKey();
    }
}