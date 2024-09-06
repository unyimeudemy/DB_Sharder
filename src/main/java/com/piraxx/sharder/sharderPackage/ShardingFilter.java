package com.piraxx.sharder.sharderPackage;

import com.fasterxml.jackson.core.JsonParser;
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

        ContentCachingRequestWrapper cachedRequest = new ContentCachingRequestWrapper(request);
        String shardKey = determineShard(getRequestBody(cachedRequest));

        System.out.println("=========================== " + shardKey);
        ShardingContextHolder.setCurrentShardKey(shardKey);
        System.out.println(")))))))))))))))))))))))))))))))))))))))");
        filterChain.doFilter(cachedRequest, response);
    }

    public TestEntity getRequestBody(ContentCachingRequestWrapper  request) throws IOException {

//        Object res = convertRequestToEntity( request, TestEntity.class);
////        String requestBody = new String(((ContentCachingRequestWrapper) request)
////                .getContentAsByteArray());
//        System.out.println("*******************************************" );
//        return (TestEntity) res;

        // Use the cached body from the wrapper
        TestEntity res = convertRequestToEntity(request, TestEntity.class);
        String requestBody = new String(request.getContentAsByteArray());



        // Convert the JSON body to TestEntity
//        objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + requestBody);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + res);
        return objectMapper.readValue(requestBody, TestEntity.class);
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
        StringBuilder stringBuilder = new StringBuilder();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(request.getInputStream()));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            stringBuilder.append(line);
        }
        String requestBody = stringBuilder.toString();

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(requestBody, clazz);
    }
}
