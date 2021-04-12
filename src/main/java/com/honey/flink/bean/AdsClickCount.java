package com.honey.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdsClickCount {
    private String province;
    private Long windowEnd;
    private Integer count;
}
