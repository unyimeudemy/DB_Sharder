package com.piraxx.sharder.sharderPackage.utils;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.security.Key;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class HandleRepositoryMethodsReponses {

    private static final Logger logger = LoggerFactory.getLogger(HandleRepositoryMethodsReponses.class);


    /* This method assumes all the repository method will return List<entity>
     * It will be replicated for all other expected return type*/
    public static Object transformResultSetToAppropriateReturnType(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();

        /* Here we get the class representing the actual return type regardless of
         * if it is parameterized or not. For example if we have List<Integer>, List is
         * returned.*/
        Class<?> returnType = method.getReturnType();


        // Check if the return type is `List<T>`
        if (returnType.equals(List.class)) {
            return responseWithList(combinedResults, joinPoint);
        }

        // Check if the return type is `S`
        if (returnType.equals(S.class)) {
            // Handle S return type logic here
        }

// Check if the return type is `Optional<T>`
        if (returnType.equals(Optional.class)) {
            // Handle Optional<T> return type logic here
        }

// Check if the return type is `boolean`
        if (returnType.equals(boolean.class) || returnType.equals(Boolean.class)) {
            // Handle boolean return type logic here
        }

// Check if the return type is `void`
        if (returnType.equals(void.class)) {
            // Handle void return type logic here
        }

        // Check if the return type is `Iterable<T>`
        if (returnType.equals(Iterable.class)) {
            // Handle Iterable<T> return type logic here
        }


// Check if the return type is `Flux<T>`
        if (returnType.equals(Flux.class)) {
            // Handle Flux<T> return type logic here
        }

// Check if the return type is `Mono<T>`
        if (returnType.equals(Mono.class)) {
            // Handle Mono<T> return type logic here
        }

        // Check if the return type is `Single<T>`
        if (returnType.equals(Single.class)) {
            // Handle Single<T> return type logic here
        }

// Check if the return type is `Completable`
        if (returnType.equals(Completable.class)) {
            // Handle Completable return type logic here
        }

// Check if the return type is `Page<T>`
        if (returnType.equals(Page.class)) {
            // Handle Page<T> return type logic here
        }

// Check if the return type is `long`
        if (returnType.equals(long.class) || returnType.equals(Long.class)) {
            // Handle long return type logic here
        }

    }

    private static Object responseWithList (List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        if(!isGenericType(joinPoint)){
            return processSimpleTypeList(combinedResults, joinPoint);
        }else{
            return processEntityTypeList(combinedResults, joinPoint);
        }
    }

    private static Object processSimpleTypeList(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Class<?> returnType = method.getReturnType();

        List<Object> responseList = new ArrayList<>();

        for(Map<String, Object> record: combinedResults){
            for(Object value: record.values()){
                if(returnType.isInstance(value)){
                    responseList.add(value);
                }else {
                    responseList.add(convertType(value, returnType));
                }
            }
        }
        return responseList;
    }

    private static Object processEntityTypeList(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();

        /* Here we get the class representing the actual return type regardless of
         * if it is parameterized or not. For example if we have List<Integer>, List is
         * returned.*/
        Class<?> returnedType = method.getReturnType();

        /* checks if the returned value is List or a subclass of List
         *  Note here that we are not using instanceOf because we do not know the exact
         *  object or instance that is coming in.*/
        if(returnedType.isAssignableFrom(List.class)){

            /* Returns the full picture of the return type including the generic type
             * and the argument that is passed into it. If we have List<Integer>, it returns
             * List<Integer>. And since the return type is a generic with a parameter, then it
             * is a parameterized type*/
            Type genericReturnType = method.getGenericReturnType();

            /* checks if the return type is generic and uses an argument*/
            if(genericReturnType instanceof ParameterizedType){

                /* If parameterized, we get the arguments that are passed into the generic*/
                ParameterizedType parameterizedType = (ParameterizedType) genericReturnType;
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();

                /* If there is an argument, we get its class name*/
                if(actualTypeArguments.length > 0){
                    Class<?> entityType = (Class<?>) actualTypeArguments[0];
                    return prepareListOfEntities(combinedResults, entityType);
                }
            }
        }
        return null;
    }

    private static List<Object> prepareListOfEntities(List<Map<String, Object>> combinedResults, Class<?> entityType){
        List<Object> responseList = new ArrayList<>();
        try {
            for(Map<String, Object> record: combinedResults){
                Object entityInstance = entityType.getDeclaredConstructor().newInstance();
                Field[] fields = entityType.getDeclaredFields();

                for(Field field: fields){

                    if(field.isAnnotationPresent(Id.class)){continue;}
                    field.setAccessible(true);

                    // Use the column name if @Column is present
                    String fieldName = field.getName();
                    if (field.isAnnotationPresent(Column.class)) {
                        Column columnAnnotation = field.getAnnotation(Column.class);
                        fieldName = removeUnderScores(columnAnnotation.name());
                    }

                      for(String key: record.keySet()){
                          if(fieldName.equalsIgnoreCase(removeUnderScores(key))){
                              Object value = record.get(key);
                              if(value != null && field.getType().isAssignableFrom(value.getClass())){
                                  field.set(entityInstance, value);
                              }else {
                                  field.set(entityInstance, convertType(value, field.getType()));
                              }
                          }
                      }
                }
                responseList.add(entityInstance);
            }
            return responseList;

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            logger.error("Error while preparing list of entities", e);
        }
        return responseList;
    }

    private static Object convertType(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }

        if (targetType.equals(String.class)) {
            return value.toString();
        }
        if (targetType.equals(Integer.class) || targetType.equals(int.class)) {
            return Integer.parseInt(value.toString());
        }
        if (targetType.equals(Long.class) || targetType.equals(long.class)) {
            return Long.parseLong(value.toString());
        }
        if (targetType.equals(Boolean.class) || targetType.equals(boolean.class)) {
            return Boolean.parseBoolean(value.toString());
        }
        if (targetType.equals(Double.class) || targetType.equals(double.class)) {
            return Double.parseDouble(value.toString());
        }
        if (targetType.equals(BigDecimal.class)) {
            return new BigDecimal(value.toString());
        }
        if (targetType.equals(LocalDateTime.class)) {
            return LocalDateTime.parse(value.toString());
        }
        if (targetType.equals(LocalDate.class)) {
            return LocalDate.parse(value.toString());
        }
        if (targetType.equals(Date.class)) {
            // Ensure the value is of type Timestamp for conversion
            if (value instanceof java.sql.Timestamp) {
                return new Date(((java.sql.Timestamp) value).getTime());
            }
        }
        if (targetType.equals(UUID.class)) {
            return UUID.fromString(value.toString());
        }

        return value;
    }

    private static String removeUnderScores(String str){
        return str.replace("_", "");
    }


    private static Boolean isGenericType(JoinPoint joinPoint){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Type genericReturnType = method.getGenericReturnType();
        return genericReturnType instanceof ParameterizedType;
    }

    private static String getClassNameFromFQCN(String FQCN){
        return FQCN.substring(FQCN.lastIndexOf('.') + 1);
    }

    public static List<Map<String, Object>> combineQueryResults(List<ResultSet> results) throws SQLException {

        /*
         * We have to loop through everything because a result set is not updatable
         * according to this portion of the documentation
         * "A default ResultSet object is not updatable and has a cursor that moves forward only."
         */
        List<Map<String, Object>> combinedResults = new ArrayList<>();
        for (ResultSet resultSet: results){
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount  = metaData.getColumnCount();

            while(resultSet.next()){
                Map<String, Object> row = new HashMap<>();

                for(int i=1; i<=columnCount; i++){
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    row.put(columnName, value);
                }
                combinedResults.add(row);
            }
        }
        return combinedResults;
    }

}
