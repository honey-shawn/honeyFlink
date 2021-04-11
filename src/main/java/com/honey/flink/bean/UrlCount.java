package com.honey.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UrlCount {
    private String url;
    private Long windowEnd;
    private Integer count;

}
