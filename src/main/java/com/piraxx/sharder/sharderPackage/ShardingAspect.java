package com.piraxx.sharder.sharderPackage;

import com.piraxx.sharder.domain.TransactionEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

@Aspect
@Component
public class ShardingAspect {

    static ConsistentHashing consistentHashing = new ConsistentHashing();

    @Before("execution(* com.piraxx..repositories..*(..))")
    private void shardingAspect(JoinPoint joinPoint) throws IllegalAccessException {
        Boolean usesRawQuery = isAnnotatedWithQuery(joinPoint);
        if(usesRawQuery){
            processRequestWithRawSqlQuery(joinPoint);
        }else {
            processRequestWithoutRawSqlQuery(joinPoint);
        }
    }

    private static void processRequestWithRawSqlQuery(JoinPoint joinPoint){
        String sqlString = getRawSqlQueryFromJointPoint(joinPoint);

        /**
         * 1. how to run raw sql on shard
         * 2. How to implement scatter method of querying shard in distributed system
         * 3. Implement Entity With UUId
         */

    }

    private static void processRequestWithoutRawSqlQuery(JoinPoint joinPoint) throws IllegalAccessException {
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
             *
             */


        }else if(args.length == 1){
            // if request comes in with only one arg like findById or save etc
            Object arg = args[0];
            if(arg instanceof String || arg instanceof  Number){
                // if the arg is just an ID like UUID or from snowflake like in findById etc
                selectShardForSingleArg(arg);
            }else{
                // if the arg is an entity like in save
                selectShardForSingleArg(arg);
            }
        }else {
            /*
             * There are situations where multiple arguments come in but only one of them
             * which may be int (or string) or an object (with an id field) that will
             * determine the shard to operate on.
             */

            selectShardForMultipleArgs(args);
        }
    }

    private static String getRawSqlQueryFromJointPoint(JoinPoint joinPoint){
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();

        if(method.isAnnotationPresent(Query.class)){
            Query queryAnnotation = method.getAnnotation(Query.class);
            return queryAnnotation.value();
        }
        return null;
    }

    private static Boolean isAnnotatedWithQuery(JoinPoint joinPoint){
        Annotation[] repositoryMethodAnnotations = getRepositoryMethodAnnotation(joinPoint);
        for(Annotation annotation: repositoryMethodAnnotations){
            if(annotation.annotationType() == Query.class){
                return true;
            }
        }
        return false;
    }

    private static Annotation[] getRepositoryMethodAnnotation(JoinPoint joinPoint){
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        return method.getAnnotations();
    }

    @After("execution (* com.piraxx.sharder.repositories..*(..))")
    private void clearShardingContext() {
        ShardingContextHolder.clear();
    }

    private static void selectShardForMultipleArgs(Object[] args){
        for (Object arg: args){
            if(arg instanceof TransactionEntity){
                TransactionEntity transactionEntity = (TransactionEntity) arg;
                String shardKey = determineShard(transactionEntity.getTransactionId());
                ShardingContextHolder.setCurrentShardKey(shardKey);
            }
        }
    }

    private static void selectShardForSingleArg(Object arg) throws IllegalAccessException {
            Class<?> clazz = arg.getClass();
            if(clazz.isAnnotationPresent(Entity.class)){
                // if argument is an entity
                String shardKey = determineShard(getIdFieldValue((arg)));
                ShardingContextHolder.setCurrentShardKey(shardKey);

            }else{
                // if argument is a string or a number
                String shardKey = determineShard(arg);
                ShardingContextHolder.setCurrentShardKey(shardKey);
            }
    }

    private static Object getIdFieldValue(Object entityInstance ) throws IllegalAccessException {

        Class<?> entityClass =  entityInstance.getClass();
        for(Field field: entityClass.getDeclaredFields()){

            if(field.isAnnotationPresent(Id.class)){
                field.setAccessible(true);
                return field.get(entityInstance);
            }
        }
        throw new IllegalArgumentException("No field annotated in class of " + entityClass.getName());
    }

    private static String determineShard(Object obj){
        return consistentHashing.getNode(obj);
    }
}
