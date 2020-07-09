package com.yyl.test03;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author yang.yonglian
 * @ClassName: com.yyl.test02
 * @Description: 提升consumer的处理速度
 * 1)提高消费者的并行度：首先 在同一个group下创建多个consumer，订阅同一个topic，consumer的数量不要超过
 * topic下的读队列的数量，否则超过consumer的实例接收不到消息 其次是设置consumer的并行处理的线程数
 * 2)设置一次拉取的消息数目
 * @Date 2019/6/7 0007
 */
public class MyConsumerForQuick {
    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("yyl_group_quick_consumer");
        consumer.setNamesrvAddr("192.168.216.145:9876;192.168.216.148:9876");
        //设置consumer的并行线程数
        consumer.setConsumeThreadMax(10);
        consumer.setConsumeThreadMin(2);
        consumer.subscribe("yyl_topic_quick","yyl_tag");
        //设置一次拉取消息的数目为3
        consumer.setConsumeMessageBatchMaxSize(3);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("msgs size:"+msgs.size());
                for(MessageExt ext:msgs){
                    System.out.println("receive msg :"+new String(ext.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
