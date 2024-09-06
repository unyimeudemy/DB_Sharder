package com.piraxx.sharder.controller;

import com.piraxx.sharder.domain.TestRequestDto;
import com.piraxx.sharder.services.TestService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.ContentCachingRequestWrapper;

@RestController
@RequestMapping("/api/test")
public class Test {

    @Autowired
    public TestService testService;

    @PostMapping("/create")
    public String testCreate(
            HttpServletRequest request
    ){
        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request);

        // Read the request body content as byte array
        String requestBody = new String(wrappedRequest.getContentAsByteArray());

        System.out.println("Request Body: " + requestBody);
        return requestBody;
    }

}
