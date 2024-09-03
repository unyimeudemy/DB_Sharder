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

import java.io.IOException;
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

        System.out.println("=========================== " + getRequestBody(request).getTransactionId().getClass().getSimpleName());
        String shardKey = determineShard(getRequestBody(request));

        System.out.println("=========================== " + shardKey);
        ShardingContextHolder.setCurrentShardKey(shardKey);

        filterChain.doFilter(request, response);
    }

    public TestEntity getRequestBody(HttpServletRequest request) throws IOException {
//        String requestBody = request.getReader().lines()
//                .collect(Collectors.joining(System.lineSeparator()));
//        System.out.println("++++++++++++++++++++++++++++" + objectMapper.readValue(requestBody, TestEntity.class));
//        return objectMapper.readValue(requestBody, TestEntity.class);

        String requestBody = new String(((ContentCachingRequestWrapper) request)
                .getContentAsByteArray());
        System.out.println("++++++++++++++++++++++++++++" + objectMapper.readValue(requestBody, TestEntity.class));
        return objectMapper.readValue(requestBody, TestEntity.class);
        // this text is to trigger commit so that I change the previous commit message
    }

    private static String determineShard(TestEntity obj){
        System.out.println("------------------------" + obj.getTransactionId());
        int transactionId = obj.getTransactionId();
        if(transactionId % 2 == 0){
            return "shard1";
        }else{
            return "shard2";
        }
    }
}
