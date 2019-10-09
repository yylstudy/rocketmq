package com.yyl.test02;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author yang.yonglian
 * @ClassName: com.yyl.test02
 * @Description: 消费部分顺序的消息
 * @Date 2019/6/7 0007
 */
public class MyConsumerForOrder {
    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_group");
        consumer.setNamesrvAddr("192.168.111.128:9876;192.168.111.129:9876");
        consumer.subscribe("order_topic","yyl_tag");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                try{
                    System.out.println("receive msg:"+new String(msgs.get(0).getBody()));
                    //顺序消息的重试：当消费者消费失败后，rocketmq会不断的进行消息重试（间隔时间为1s）
                    //其他消息将处于被阻塞状态，直到这条消息被消费成功，因此对于顺序消息的重试，务必保证
                    //应用能够及时监控并处理消费失败的情况，避免阻塞现象的发生
//                    if("hello rocketmq1".equals(new String(msgs.get(0).getBody()))){
//                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
//                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }catch (Exception e){
                    System.out.println("消息消费失败");
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }

            }
        });
        consumer.start();
    }
}
