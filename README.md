# HoneyFlink

#### 介绍
> Flink项目的理念是：“Apache Flink是为分布式、高性能、随时可用以及准确的流处理应用程序打造的开源流处理框架”。

> Apache Flink是一个框架和分布式处理引擎，用于对无界和有界数据流进行有状态计算。Flink被设计在所有常见的集群环境中运行，以内存执行速度和任意规模来执行计算。


#### 软件架构
*	Flink灵活的窗口
*	Exactly Once语义保证


#### 内容介绍

1.  com.atguigu包为SSG关于Flink的原生代码
2.  com.honey.flink为作者的学习编辑代码


#### 目录
* 第1章	Flink简介	1

    * 1.1	初识Flink	1
    * 1.2	Flink的重要特点	2
    * 1.2.1	事件驱动型(Event-driven)	2
    * 1.2.2	流与批的世界观	3
    * 1.2.3	分层API	4
    * 1.3	Spark or Flink	5
* 第2章	Flink快速上手	6

    * 2.1	创建maven项目	6
    * 2.2	批处理WordCount	7
    * 2.3	流处理WordCount	8
    * 2.3.1	有界流	8
    * 2.3.2	无界流	8
* 第3章	Flink部署	9
    
    * 3.1	开发模式	9
    * 3.2	local-cluster模式	9
    * 3.2.1	local-cluster模式配置	9
    * 3.2.2	在local-cluster模式下运行无界的WordCount	9
    * 3.3	Standalone模式	11
    * 3.3.1	Standalone模式配置	11
    * 3.3.2	Standalone模式运行无界流WorkCount	11
    * 3.3.3	Standalone高可用(HA)	12
    * 3.4	Yarn模式	13
    * 3.4.1	Yarn模式配置	13
    * 3.4.2	Yarn运行无界流WordCount	13
    * 3.4.3	Flink on Yarn的3种部署模式	14
    * 3.4.4	Per-Job-Cluster模式执行无界流WordCount	16
    * 3.4.5	Session-Cluster模式执行无界流WordCount	16
    * 3.4.6	Application Mode模式执行无界流WordCount	16
    * 3.4.7	Yarn模式高可用	16
    * 3.5	Scala REPL	17
    * 3.6	K8S & Mesos模式	17
* 第4章	Flink运行架构	18

    * 4.1	运行架构	18
    * 4.1.1	客户端	18
    * 4.1.2	JobManager	18
    * 4.1.3	TaskManager	20
    * 4.2	核心概念	20
    * 4.2.1	TaskManager与Slots	20
    * 4.2.2	Parallelism（并行度）	21
    * 4.2.3	Task与SubTask	22
    * 4.2.4	Operator Chains（任务链）	22
    * 4.2.5	ExecutionGraph（执行图）	22
    * 4.3	提交流程	24
    * 4.3.1	高级视角提交流程(通用提交流程)	24
    * 4.3.2	yarn-cluster提交流程per-job	25
* 第5章	Flink流处理核心编程（重点）	25
    
    * 5.1	Environment	26
    * 5.2	Source	26
    * 5.2.1	准备工作	26
    * 5.2.2	从Java的集合中读取数据	27
    * 5.2.3	从文件读取数据	27
    * 5.2.4	从Socket读取数据	28
    * 5.2.5	从Kafka读取数据	28
    * 5.2.6	自定义Source	29
    * 5.3	Transform	30
    * 5.3.1	map	30
    * 5.3.2	flatMap	32
    * 5.3.3	filter	33
    * 5.3.4	keyBy	34
    * 5.3.5	shuffle	35
    * 5.3.6	split和select	36
    * 5.3.7	connect	37
    * 5.3.8	union	38
    * 5.3.9	简单滚动聚合算子	39
    * 5.3.10	reduce	41
    * 5.3.11	process	42
    * 5.3.12	对流重新分区的几个算子	43
    * 5.4	Sink	44
    * 5.4.1	KafkaSink	44
    * 5.4.2	RedisSink	46
    * 5.4.3	ElasticsearchSink	47
    * 5.4.4	自定义Sink	49
    * 5.5	执行模式(Execution Mode)	51
    * 5.5.1	选择执行模式	51
    * 5.5.2	配置BATH执行模式	51
    * 5.5.3	有界数据用STREAMING和BATCH的区别	52
* 第6章	Flink流处理核心编程实战	53
    
    * 6.1	基于埋点日志数据的网络流量统计	53
    * 6.1.1	网站总浏览量（PV）的统计	53
    * 6.1.2	网站独立访客数（UV）的统计	55
    * 6.2	市场营销商业指标统计分析	56
    * 6.2.1	APP市场推广统计 - 分渠道	56
    * 6.2.2	APP市场推广统计 - 不分渠道	57
    * 6.3	各省份页面广告点击量实时统计	57
    * 6.4	订单支付实时监控	59
* 第7章	Flink流处理高阶编程（重点）	61
    
    * 7.1	Flink的window机制	61
    * 7.1.1	窗口概述	61
    * 7.1.2	窗口的分类	61
    * 7.1.3	Window Function	66
    * 7.2	Keyed vs Non-Keyed Windows	67
    * 7.3	Flink中的时间语义与WaterMark	68
    * 7.3.1	Flink中的时间语义	68
    * 7.3.2	哪种时间更重要	69
    * 7.3.3	Flink中的WaterMark	70
    * 7.3.4	Flink中如何产生水印	71
    * 7.3.5	EventTime和WaterMark的使用	71
    * 7.3.6	自定义WatermarkStrategy	72
    * 7.3.7	多并行度下WaterMark的传递	75
    * 7.4	窗口允许迟到的数据	75
    * 7.5	侧输出流(sideOutput)	75
    * 7.5.1	处理窗口关闭之后的迟到数据	75
    * 7.5.2	使用侧输出流把一个流拆成多个流	77
    * 7.6	ProcessFunction API(底层API)	77
    * 7.6.1	ProcessFunction	77
    * 7.6.2	KeyedProcessFunction	78
    * 7.6.3	CoProcessFunction	78
    * 7.6.4	ProcessJoinFunction	78
    * 7.6.5	BroadcastProcessFunction	79
    * 7.6.6	KeyedBroadcastProcessFunction	79
    * 7.6.7	ProcessWindowFunction	79
    * 7.6.8	ProcessAllWindowFunction	79
    * 7.7	定时器	79
    * 7.7.1	基于处理时间的定时器	80
    * 7.7.2	基于事件时间的定时器	80
    * 7.7.3	定时器练习	81
    * 7.8	Flink状态编程	82
    * 7.8.1	什么是状态	82
    * 7.8.2	为什么需要管理状态	83
    * 7.8.3	Flink中的状态分类	83
    * 7.8.4	Managed State的分类	83
    * 7.8.5	算子状态的使用	84
    * 7.8.6	键控状态的使用	87
    * 7.8.7	状态后端	92
    * 7.9	Flink的容错机制	94
    * 7.9.1	状态的一致性	94
    * 7.9.2	Checkpoint原理	96
    * 7.9.3	Savepoint原理	101
    * 7.9.4	checkpoint和savepoint的区别	101
    * 7.9.5	Flink+Kafka 实现端到端严格一次	101
    * 7.9.6	在代码中测试Checkpoint	102
    
* 第8章	Flink流处理高阶编程实战	104
    
    * 8.1	基于埋点日志数据的网络流量统计	104
    * 8.1.1	指定时间范围内网站总浏览量（PV）的统计	104
    * 8.1.2	指定时间范围内网站独立访客数（UV）的统计	105
    * 8.2	电商数据分析	106
    * 8.2.1	实时热门商品统计	107
    * 8.2.2	基于服务器log的热门页面浏览量统计	109
    * 8.3	页面广告分析	112
    * 8.3.1	页面广告点击量统计	112
    * 8.3.2	黑名单过滤	114
    * 8.4	恶意登录监控	116
    * 8.4.1	数据源	117
    * 8.4.2	封装数据的JavaBean类	117
    * 8.4.3	具体实现代码	117
    * 8.5	订单支付实时监控	119
    
* 第9章	Flink CEP编程	121
    
    * 9.1	什么是FlinkCEP	121
    * 9.2	Flink CEP应用场景	122
    * 9.3	CEP开发基本步骤	122
    * 9.3.1	导入CEP相关依赖	122
    * 9.3.2	基本使用	122
    * 9.4	模式API	123
    * 9.4.1	单个模式	124
    * 9.4.2	组合模式(模式序列)	126
    * 9.4.3	模式知识补充	128
    * 9.4.4	模式组	130
    * 9.4.5	超时数据	130
    * 9.4.6	匹配后跳过策略	131
* 第10章	Flink CEP编程实战	131

    * 10.1	恶意登录监控	131
    * 10.2	订单支付实时监控	132


