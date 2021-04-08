package com.honey.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UVCount {
    private String uv;
    private String time;
    private Integer count;
}
