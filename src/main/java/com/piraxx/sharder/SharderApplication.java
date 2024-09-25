package com.piraxx.sharder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
/* ensure that it is not already enabled in a typical spring boot app
* and if not present, check if there is another way providing the functionality
* of the annotation without the annotation*/
@EnableJpaRepositories
public class SharderApplication {
	public static void main(String[] args) {
		SpringApplication.run(SharderApplication.class, args);
	}

}
