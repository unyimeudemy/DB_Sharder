package com.piraxx.sharder.sharderPackage.utils;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.math.BigDecimal;
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

        // Check if the return type is `Optional<T>`
        if (returnType.equals(Optional.class)) {
            return responseWithOptional(combinedResults, joinPoint);
        }

        // Check for those returning just the entity
        if (isEntityClass(joinPoint)) {
            return responseWithEntityClass(combinedResults, joinPoint);
        }


// Check if the return type is `boolean`
        if (returnType.equals(boolean.class) || returnType.equals(Boolean.class)) {
            // Handle boolean return type logic here
        }

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

    private static Object responseWithEntityClass(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        Class<?> entityType = getNoneParameterizedReturnType(joinPoint);
        try{
            return buildEntity(entityType, combinedResults);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e){
            logger.error("Error while preparing entity", e);
        }
        return Optional.empty();
    }

    private static Object responseWithOptional(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        if(isGenericParameterSimpleNotEntity(joinPoint)){
            return processSimpleTypeOptional(combinedResults, joinPoint);
        }else {
            return processEntityTypeOptional(combinedResults, joinPoint);
        }
    }

    private static Object responseWithList (List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        if(isGenericParameterSimpleNotEntity(joinPoint)){
            return processSimpleTypeList(combinedResults, joinPoint);
        }else{
            return processEntityTypeList(combinedResults, joinPoint);
        }
    }

    private static Object processSimpleTypeOptional(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        Class<?> type = getArgInParameterizedReturnType(joinPoint);
        for(Map<String, Object> record: combinedResults){
            for(Object value: record.values()){
                if(value != null){
                    if(type.isInstance(value)){
                        return Optional.of(value);
                    }else {
                        return Optional.of(convertType(value, type));
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static Object processEntityTypeOptional(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        Class<?> entityType = getArgInParameterizedReturnType(joinPoint);
        return prepareOptionalOfEntity(combinedResults,entityType);
    }

    private static Object processSimpleTypeList(List<Map<String, Object>> combinedResults, JoinPoint joinPoint ){
        Class<?> returnType = getArgInParameterizedReturnType(joinPoint);

        List<Object> responseList = new ArrayList<>();

        for(Map<String, Object> record: combinedResults){
            for(Object value: record.values()){
               if(value != null){
                   if(returnType.isInstance(value)){
                       responseList.add(value);
                   }else {
                       responseList.add(convertType(value, returnType));
                   }
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
                    return prepareListOfEntity(combinedResults, entityType);
                }
            }
        }
        return null;
    }

    private static Optional<Object> prepareOptionalOfEntity(List<Map<String, Object>> combinedResults, Class<?> entityType){
        try{
            Object entityInstance = buildEntity(entityType, combinedResults);
            return Optional.of(entityInstance);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e){
            logger.error("Error while preparing entity for Optional", e);
        }
        return Optional.empty();
    }

    private static List<Object> prepareListOfEntity(List<Map<String, Object>> combinedResults, Class<?> entityType){
        List<Object> responseList = new ArrayList<>();
        try {
            Field[] fields = entityType.getDeclaredFields();
            for(Map<String, Object> record: combinedResults){
                Object entityInstance = entityType.getDeclaredConstructor().newInstance();

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

    private static Boolean isGenericParameterSimpleNotEntity(JoinPoint joinPoint){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Type genericReturnType = method.getGenericReturnType();
        ParameterizedType parameterizedType = (ParameterizedType) genericReturnType;
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        return checkIfArgumentIsSimpleNotEntity(actualTypeArguments[0]);
    }

    private static Boolean checkIfArgumentIsSimpleNotEntity(Type type){
        if (type instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type;

            return clazz.equals(String.class) ||
                    clazz.equals(Integer.class) || clazz.equals(int.class) ||
                    clazz.equals(Long.class) || clazz.equals(long.class) ||
                    clazz.equals(Boolean.class) || clazz.equals(boolean.class) ||
                    clazz.equals(Double.class) || clazz.equals(double.class) ||
                    clazz.equals(BigDecimal.class) ||
                    clazz.equals(LocalDateTime.class) ||
                    clazz.equals(LocalDate.class) ||
                    clazz.equals(Date.class) ||
                    clazz.equals(UUID.class);
        }
        return false;
    }

    private static Class<?> getArgInParameterizedReturnType(JoinPoint joinPoint){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        ParameterizedType parameterizedType = (ParameterizedType) method.getGenericReturnType();
        Type[] types = parameterizedType.getActualTypeArguments();
        return (Class<?>) types[0];
    }

    private static Class<?> getNoneParameterizedReturnType(JoinPoint joinPoint){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        return method.getReturnType();
    }

    private static Boolean isEntityClass(JoinPoint joinPoint){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Class<?> type = method.getReturnType();
        /* Class<?> is an implementation of Type which is an interface. So if you
        * try to get the isAnnotationPresent on Type there will be error.
        *
        * Also note, if you do getClass() on Type, you are not getting the return type
        * in the method, but you are getting the class
        * object for the object type, which is already a Class<?>. You are
        * */
        return type.isAnnotationPresent(Entity.class);
    }

    private static Object buildEntity(Class<?> entityType, List<Map<String, Object>> combinedResults) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Object entityInstance = entityType.getDeclaredConstructor().newInstance();
        Field[] declaredFields = entityType.getDeclaredFields();

        for(Map<String, Object> record: combinedResults) {
            for (Field field : declaredFields) {

                if (field.isAnnotationPresent(Id.class)) {
                    continue;
                }
                field.setAccessible(true);

                String fieldName = field.getName();
                if (field.isAnnotationPresent(Column.class)) {
                    Column columnAnnotation = field.getAnnotation(Column.class);
                    fieldName = removeUnderScores(columnAnnotation.name());
                }

                for (String key : record.keySet()) {
                    if (fieldName.equalsIgnoreCase(removeUnderScores(key))) {
                        Object value = record.get(key);
                        if (value != null && field.getType().isAssignableFrom(value.getClass())) {
                            field.set(entityInstance, value);
                        } else {
                            field.set(entityInstance, convertType(value, field.getType()));
                        }
                    }
                }
            }
        }
        return entityInstance;
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
