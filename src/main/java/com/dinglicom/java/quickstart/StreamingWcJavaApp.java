package com.dinglicom.java.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWcJavaApp {


    @SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

        // step1 ：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);


        // step3: transform

        //text.flatMap(new MyFlatMapFunction())   //.keyBy("word")
        text.flatMap(new RichFlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new WordCount(token.trim(), 1));
                    }
                }
            }
        })
                .keyBy(new KeySelector<WordCount, String>() {

                    @Override
                    public String getKey(WordCount wc) throws Exception {
                        return wc.word;
                    }
                }).timeWindow(Time.seconds(5))
                .sum("count").print()
                .setParallelism(1);


        env.execute("StreamingWCJavaApp");
    }


    public static class MyFlatMapFunction implements FlatMapFunction<String, WordCount> {

        /**
		 * 
		 */
		private static final long serialVersionUID = -3669660395248165202L;

		@Override
        public void flatMap(String value, Collector<WordCount> collector) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new WordCount(token.trim(), 1));
                }
            }
        }
    }

    public static class WordCount {
        private String word;
        private int count;

        public WordCount() {
        }

        public WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

}
