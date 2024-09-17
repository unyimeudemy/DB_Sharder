package com.piraxx.sharder.sharderPackage;


import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


@Aspect
@Component
public class DataSourcesHandlerAspect {

    // List to store all the database shards
    Object[] args;

    // base name for database shard to which each shard unique identifier
    // will be appended to.
    String shardsBaseName = "shard";

    // Hashmap to contain datasources and their shard names
    static Map<Object, Object> dataSourceMap = new HashMap<>();


    public  Object[] getShardList(){
        return args;
    }

    public Map<Object, Object> getDataSourceMap(){
        return dataSourceMap;
    }

    public ShardingDataSource setDataSources(DataSource defaultDataSource){
        ShardingDataSource shardingDataSource = new ShardingDataSource();
        shardingDataSource.setTargetDataSources(dataSourceMap);

        // Set a default data source
        shardingDataSource.setDefaultTargetDataSource(defaultDataSource);
        return shardingDataSource;
    }


    @Before("execution(* com.piraxx.sharder..configs..DataSourceConfig.shardingDataSource(..))")
    private void mapDataSource(JoinPoint joinPoint){
        args = joinPoint.getArgs();
        int shardCount = 1;
        for(Object arg: args){
            dataSourceMap.put(shardsBaseName + shardCount, arg);
            shardCount++;
        }
    }
}
