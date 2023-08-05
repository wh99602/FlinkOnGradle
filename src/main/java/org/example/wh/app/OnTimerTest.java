package org.example.wh.app;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class OnTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketSource = env.socketTextStream("43.143.254.23", 7777);

        socketSource
                .keyBy(r->r.split(",")[0])
                .process(new KeyedProcessFunction<String, String, String>() {
                    private ValueState<Integer> value;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        value=getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                                "value", Types.INT
                        ));
                    }

                    @Override
                    public void processElement(String value,Context ctx, Collector<String> out) throws Exception {
                        TimerService timerService = ctx.timerService();
                        long ts = timerService.currentProcessingTime();

                        timerService.registerProcessingTimeTimer(ts+5000L);
                        timerService.registerProcessingTimeTimer(ts+10000L);
                        timerService.registerProcessingTimeTimer(ts+15000L);
                    }

                    @Override
                    public void onTimer(long timestamp,OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("定时器触发啦" + timestamp+ctx.getCurrentKey());
                        if(value.value()==null){
                            value.update(0);
                        }
                        value.update(value.value()+1);
                        if(value.value()==3){
                            System.out.println("第三次啦");
                        }
                    }
                });

        env.execute();
    }
}
