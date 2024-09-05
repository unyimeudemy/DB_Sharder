package com.piraxx.sharder.sharderPackage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.piraxx.sharder.domain.TestEntity;
import com.piraxx.sharder.domain.TestRequestDto;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

@Component
public class ShardingFilter extends OncePerRequestFilter {

    private final ObjectMapper objectMapper = new ObjectMapper();

//    private final ConsistentHashing ;
//
//    public ShardingFilter(ConsistentHashing consistentHashing) {
//        this.consistentHashing = consistentHashing;
//    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
//        try {
//            String transactionId = request.getParameter("transactionId"); // Or extract from request body
//
//            if (transactionId != null) {
////                long id = Long.parseLong(transactionId);
////                String shardKey = consistentHashing.getShardForId(id);
////                ShardingContextHolder.setCurrentShardKey(shardKey);
//            }
//
//            filterChain.doFilter(request, response);
//        } finally {
//            ShardingContextHolder.clear();
//        }

        String shardKey = determineShard(getRequestBody(request));

        System.out.println("=========================== " + shardKey);
        ShardingContextHolder.setCurrentShardKey(shardKey);
        System.out.println(")))))))))))))))))))))))))))))))))))))))");
        filterChain.doFilter(request, response);
    }

    public TestEntity getRequestBody(HttpServletRequest request) throws IOException {
//        String requestBody = request.getReader().lines()
//                .collect(Collectors.joining(System.lineSeparator()));
//        return objectMapper.readValue(requestBody, TestEntity.class);


//        System.out.println("===============> ============> " + request.getInputStream().toString());
//        String requestBody = request.getInputStream().toString();
//        return objectMapper.readValue(requestBody, TestEntity.class);

        Object res = convertRequestToEntity(request, TestEntity.class);
        System.out.println("*******************************************" + res);
        return (TestEntity) res;
    }

    private static String determineShard(TestEntity obj){
        System.out.println("===============> ============> " + obj);

        System.out.println("------------------------" + obj.getTransactionId());
        int transactionId = obj.getTransactionId();
        if(transactionId % 2 == 0){
            return "shard1";
        }else{
            return "shard2";
        }
    }

    public static <T> T convertRequestToEntity(HttpServletRequest request, Class<T> clazz) throws IOException {
        // Step 1: Read InputStream into a String
        StringBuilder stringBuilder = new StringBuilder();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(request.getInputStream()));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            stringBuilder.append(line);
        }
        String requestBody = stringBuilder.toString();

        // Step 2: Convert the JSON String to the desired entity class
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(requestBody, clazz);  // Convert JSON to specified type
    }
}
