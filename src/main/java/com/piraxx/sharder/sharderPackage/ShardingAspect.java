package com.piraxx.sharder.sharderPackage;

import com.piraxx.sharder.domain.TransactionEntity;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ShardingAspect {


    @Before("execution (* com.piraxx.sharder.repositories..*(..))")
    private void shardingAspect(JoinPoint joinPoint){
        Object[] args = joinPoint.getArgs();
        for (Object arg: args){
            System.out.println("====================================" + arg.toString());
            if(arg instanceof TransactionEntity){
                TransactionEntity transactionEntity = (TransactionEntity) arg;
                String shardKey = determineShard(transactionEntity.getTransactionId());
                System.out.println("+++++++++++++++++++++++++++++++" + shardKey);
                ShardingContextHolder.setCurrentShardKey(shardKey);
            }
        }
    }

    @After("execution (* com.piraxx.sharder.repositories..*(..))")
    private void clearShardingContext(JoinPoint joinPoint) {
        ShardingContextHolder.clear();
        System.out.println("Sharding context cleared");
    }


    private static String determineShard(Integer transactionId){
        if(transactionId % 2 == 0){
            return "shard1";
        }else{
            return "shard2";
        }
    }
}
