package com.piraxx.sharder.controller;

import com.piraxx.sharder.domain.TransactionRequestDto;
import com.piraxx.sharder.services.TransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transaction")
public class TransactionController {

    @Autowired
    public TransactionService transactionService;

    @PostMapping("/create")
    public String testCreate(
            @RequestBody TransactionRequestDto transactionRequestDto
    ){
        return transactionService.create(transactionRequestDto);
    }
}
