package com.piraxx.sharder.domain;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionRequestDto {

    private Integer transactionId;

    private String transactionDetail;
}
