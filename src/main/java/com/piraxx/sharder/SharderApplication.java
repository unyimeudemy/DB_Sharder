package com.piraxx.sharder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableJpaRepositories // ensure that it is not already enabled in a typical spring boot app
public class SharderApplication {
	public static void main(String[] args) {
		SpringApplication.run(SharderApplication.class, args);
	}

}
