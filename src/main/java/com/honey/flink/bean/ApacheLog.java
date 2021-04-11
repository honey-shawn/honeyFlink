package com.honey.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLog {
    private String ip;
    private String userId;
    private Long ts;
    private String method;
    private String url;
}
