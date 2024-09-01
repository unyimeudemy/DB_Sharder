package com.piraxx.sharder.services;

import com.piraxx.sharder.domain.TestEntity;
import com.piraxx.sharder.domain.TestRequestDto;
import com.piraxx.sharder.repositories.TestRepository;
import com.piraxx.sharder.sharderPackage.ShardingContextHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TestService {

    @Autowired
    public TestRepository testRepository;

    public String create(TestRequestDto testRequest){
        String shardKey = determineShard(testRequest.getTransactionId());
        ShardingContextHolder.setCurrentShardKey(shardKey);

         testRepository.save(
                TestEntity.builder()
                        .transactionId(testRequest.getTransactionId())
                        .transactionDetail(testRequest.getTransactionDetail() + getDbShard(testRequest.getTransactionId()))
                        .build()
        );
         return "Test detail: " + testRequest.getTransactionDetail() + + getDbShard(testRequest.getTransactionId());
    }

    private static String determineShard(int transactionId){
        if(transactionId % 2 == 0){
            return "shard1";
        }else{
            return "shard2";
        }
    }

    private static int getDbShard(int id){
        return id % 2 == 0 ? 1 : 2;
    }


}
