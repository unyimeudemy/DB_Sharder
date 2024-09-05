package com.piraxx.sharder.controller;

import com.piraxx.sharder.domain.TestRequestDto;
import com.piraxx.sharder.services.TestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/test")
public class Test {

    @Autowired
    public TestService testService;

    @PostMapping("/create")
    public String testCreate(
            @RequestBody TestRequestDto testRequest
    ){
//        System.out.println("------------------ controller");
//        Object res = testService.create(testRequest);
        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&" + testRequest.toString() );
//        return (String) res;

        return testService.create(testRequest);
    }

}
