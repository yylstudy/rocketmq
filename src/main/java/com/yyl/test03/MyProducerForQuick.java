package com.yyl.test03;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
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
public class MyProducerForQuick {
    public static void main(String[] args) throws Exception{
        //指定group名称，用于关联group上的所有的Producer
        //Producer的groupName和Consumer的groupName必须要不一样，否则消息消费会出现消费不完全或者消费不到的情况
        DefaultMQProducer producer = new DefaultMQProducer("yyl_group_quick_producer");
        //当一个jvm需要启动多个producer时，通过设置不同的instanceName来区分，不设置的话默认为"DEFAULT"
        producer.setInstanceName("instance1");
        //设置发送失败消息重试次数
        producer.setRetryTimesWhenSendFailed(3);
        //设置namesrv的地址
        producer.setNamesrvAddr("192.168.216.145:9876;192.168.216.148:9876");

        producer.start();
        List<Message> messages = new ArrayList<>();
        for(int i=0;i<10;i++){
            Message message = new Message("yyl_topic_quick",
                    "yyl_tag","hello rocketmq".getBytes(RemotingHelper.DEFAULT_CHARSET));
            messages.add(message);
        }
        SendResult sendResult = producer.send(messages);
        System.out.println(sendResult);
    }
}
