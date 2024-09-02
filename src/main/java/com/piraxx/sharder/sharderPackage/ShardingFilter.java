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

import java.io.IOException;
import java.util.stream.Collectors;

@Component
public class ShardingFilter extends OncePerRequestFilter {

    private final ObjectMapper objectMapper = new ObjectMapper();

//    private final ConsistentHashing consistentHashing;
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

        String shardKey = determineShard(getRequestBody(request).getTransactionId());
        ShardingContextHolder.setCurrentShardKey(shardKey);
    }

    public TestEntity getRequestBody(HttpServletRequest request) throws IOException {
        String requestBody = request.getReader().lines()
                .collect(Collectors.joining(System.lineSeparator()));
        return objectMapper.readValue(requestBody, TestEntity.class);
    }

    private static String determineShard(int transactionId){
        if(transactionId % 2 == 0){
            return "shard1";
        }else{
            return "shard2";
        }
    }
}
