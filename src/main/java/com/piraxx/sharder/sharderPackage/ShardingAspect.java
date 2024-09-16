package com.piraxx.sharder.sharderPackage;

import com.piraxx.sharder.domain.TransactionEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.SortedMap;

@Aspect
@Component
public class ShardingAspect {


    @Before("execution(* com.piraxx..repositories..*(..))")
    private void shardingAspect(JoinPoint joinPoint) throws IllegalAccessException {
        Object[] args = joinPoint.getArgs();

        if(args.length == 0){
            // if request comes without arg, like findAll

            /**
             * To implement query broadcasting to all shards, perform query on each
             * shard and join the result as shown
             * select * from db1.t1
             * union
             * select * from db2.t2
             *
             * The main problem is that if you run into is cross server joins,
             * on large million + row systems, it can hit the network pretty
             * hard and take a long time to process queries.
             */


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

    private static void simpleSelectShardSingleItem(Object arg) throws IllegalAccessException {

            if(arg instanceof Entity){
                // if argument is an entity
                String shardKey = determineShard(getIdFieldValue((Class<?>) arg));
                ShardingContextHolder.setCurrentShardKey(shardKey);

            }else{
                // if argument is a string or a number
                String shardKey = determineShard(arg);
                ShardingContextHolder.setCurrentShardKey(shardKey);
            }
    }

    private static Object getIdFieldValue(Class<?> entityClass ) throws IllegalAccessException {
        for(Field field: entityClass.getDeclaredFields()){
            if(field.isAnnotationPresent(Id.class)){
                return field.get(entityClass);
            }
        }
        throw new IllegalArgumentException("No field annotated in class of " + entityClass.getName());
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
