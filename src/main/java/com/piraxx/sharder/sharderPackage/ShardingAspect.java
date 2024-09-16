package com.piraxx.sharder.sharderPackage;

import com.piraxx.sharder.domain.TransactionEntity;
import jakarta.persistence.Entity;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

import java.util.SortedMap;

@Aspect
@Component
public class ShardingAspect {


    @Before("execution(* com.piraxx..repositories..*(..))")
    private void shardingAspect(JoinPoint joinPoint){
        Object[] args = joinPoint.getArgs();

        if(args.length == 0){
            // if request comes without arg, like findAll


        }else if(args.length == 1){
            // if request comes in with only one arg like findById or save etc
            Object arg = args[0];
            if(arg instanceof String || arg instanceof  Number){
                // if the arg is just an ID like UUID or from snowflake like in findById etc
                simpleSelectShardSingleItem(arg);
            }else{
                // if the arg is an entity like in save
                simpleSelectShardSingleItem(arg);
            }
        }else {
            simpleSelectShardArr(args);
        }
    }

    @After("execution (* com.piraxx.sharder.repositories..*(..))")
    private void clearShardingContext(JoinPoint joinPoint) {
        ShardingContextHolder.clear();
        System.out.println("Sharding context cleared");
    }

    private static void simpleSelectShardArr(Object[] args){
        for (Object arg: args){
            if(arg instanceof TransactionEntity){
                TransactionEntity transactionEntity = (TransactionEntity) arg;
                String shardKey = determineShard(transactionEntity.getTransactionId());
                ShardingContextHolder.setCurrentShardKey(shardKey);
            }
        }
    }

    private static void simpleSelectShardSingleItem(Object arg){

            if(arg instanceof Entity){
                //argument is an entity

                Object entity = entityList(arg);
                TransactionEntity transactionEntity = (TransactionEntity) arg;
                String shardKey = determineShard(transactionEntity.getTransactionId());
                ShardingContextHolder.setCurrentShardKey(shardKey);

            }else{
                // if argument is a string or a number
                String shardKey = determineShard(arg);
                ShardingContextHolder.setCurrentShardKey(shardKey);
            }
    }

    private static Object entityList(Object arg){
        // scans through a
    }

    private static getIdField(){

    }




    private static String determineShard(Object obj){
        int id = obj.hashCode();
        if(id % 2 == 0){
            return "shard1";
        }else{
            return "shard2";
        }
    }

}
