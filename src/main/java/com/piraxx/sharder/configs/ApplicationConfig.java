package com.piraxx.sharder.configs;

import com.piraxx.sharder.sharderPackage.ConsistentHashing;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig {

    @Bean
    public ConsistentHashing consistentHashing(){
        return new ConsistentHashing(5);
    }
}
