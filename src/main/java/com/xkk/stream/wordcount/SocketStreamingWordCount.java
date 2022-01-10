package com.xkk.stream.wordcount;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// Flink 编程模型:
// 1. 创建 Flink 运行环境
// 2. 为运行环境添加数据源
// 3. 为 DataFlow 添加一个或者多个算子
// 4. 将运算之后的结果发送到何处
// 5. 执行 Flink 程序
// 其中 算子和 sink 必须要有其中的一个

// 从 socket 流中说去输入数据
// 如果输入多个字符, 按照空格切分, 将切分之后的结果按照 单词进行重新物理分组, 分组完成之后, 计算每个输入单词的总数
// 此例子中没有统计输入的状态
//
public class SocketStreamingWordCount {

    public static void main(String[] args) throws Exception {
        // 从程序运行参数中初始化 ParameterTool 对象
        ParameterTool params = ParameterTool.fromArgs(args);

        // 获取输入参数中的 hostname 的值, 默认值是 hadoop1
        String host = params.get("hostname", "hadoop1");

        // 从输入参数中获取 port 的值, 默认值是 6666
        int port = params.getInt("port", 6666);

        // 从输入参数中获取 flink 程序的并行度,  默认值是 4
        int parallel = params.getInt("parallel", 4);

        // 1. 获取 flink 程序的可执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 禁用 operatorChaining 优化
        env.disableOperatorChaining();

        // 设置程序的并行度, 默认值是 4
        env.setParallelism(parallel);

        // 2. 添加程序的 socket 输入源, 遇到换行符号作为一个流输入对象
        DataStreamSource<String> socket = env.socketTextStream(host, port, "\n");

        // 3. 添加程序的算子
        socket.flatMap(
                // 将上一个算子的输入参数扁平化,
                // @param String 上一个算子的输入对象为 String
                // @param Tuple2<String,Integer>  当前算子的输出对象为 Tuple2<String,Integer> 的元组对象
                new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(
                            String input, // 上一个算子的输入参数
                            Collector<Tuple2<String, Integer>> out // flatMap 算子的输出对象
                    ) throws Exception {
                        // 将上一个算子的输出值, 按照空格进行切分, 然后返回一个元组对象,
                        // 元组的第一个元素表示切分之后的单词
                        // 元组的第二个元素表示切分之后, 产生单词的个数
                        for (String s : input.split(" ")) {
                            if (s.equals("")) {
                                continue;
                            }
                            // 将单词切分后, 返回单词和单词的个数
                            out.collect(Tuple2.of(s, 1));
                        }
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }
                    // keyBy 算子, 功能是将算子的输入进行分区
                    // 第一个参数为 上一个算子的输出对象的类型
                    // 第二个参数为 选择 key 的元素作为物理分区的键
                    // 内部会计算 key 的 hash (murmurhash) 然后再 模除并行度的方式得到物理分区
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> input) throws Exception {
                // 这里选择按照单词进行分组
                // 由于这里使用的是 Tuple2<String,Integer>, 元组的第一个元素为 单词, 第二个元素为单词的个数
                return input.f0;
            }
        }).sum(1)
                // 4. sink
                .print("word_count");

        // 5. 执行 Flink 程序
        env.execute("stream_word_count");

    }
}
