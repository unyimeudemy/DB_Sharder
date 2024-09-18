package com.piraxx.sharder.configs;

import com.piraxx.sharder.sharderPackage.DataSourcesHandlerAspect;
import com.piraxx.sharder.sharderPackage.ShardingDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DataSourceConfig {



    @Bean
    @Primary
    public DataSource shardingDataSource(
            @Qualifier("shard1DataSource") DataSource shard1DataSource,
            @Qualifier("shard2DataSource") DataSource shard2DataSource
    ) {
        DataSourcesHandlerAspect dataSourceHandler = new DataSourcesHandlerAspect();
        return dataSourceHandler.setDataSources(shard2DataSource);
    }

    @Bean(name = "shard1DataSource")
    public DataSource shard1DataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://dpg-cr9plvggph6c73d3ping-a.oregon-postgres.render.com:5432/shard_1");
        dataSource.setUsername("shard_1");
        dataSource.setPassword("cWApl8reqUplwP2xHkwOGiyYIGu2g4Ik");
        return dataSource;
    }

    @Bean(name = "shard2DataSource")
    public DataSource shard2DataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://dpg-cr9pmb2j1k6c73bjumq0-a.oregon-postgres.render.com:5432/shard_2");
        dataSource.setUsername("shard_2");
        dataSource.setPassword("2oFAnG8ZeJOscvcdPVsSGZ0q1RWYZevg");
        return dataSource;
    }
}
