package com.piraxx.sharder.services;

import com.piraxx.sharder.domain.TransactionEntity;
import com.piraxx.sharder.domain.TransactionRequestDto;
import com.piraxx.sharder.repositories.TransactionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TransactionService {

    @Autowired
    public TransactionRepository transactionRepository;

    public String create(TransactionRequestDto testRequest){
        System.out.println("-----------------------------" + testRequest.toString());
         transactionRepository.save(
                TransactionEntity.builder()
                        .transactionId(testRequest.getTransactionId())
                        .transactionDetail(testRequest.getTransactionDetail() + getDbShard(testRequest.getTransactionId()))
                        .build()
        );
         return "Test detail: " + testRequest.getTransactionDetail() + getDbShard(testRequest.getTransactionId());
    }

    private static int getDbShard(int id){
        return id % 2 == 0 ? 1 : 2;
    }
}
