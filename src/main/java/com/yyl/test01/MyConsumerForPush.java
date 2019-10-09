package com.yyl.test01;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @Author yang.yonglian
 * @ClassName: com.yyl.test01
 * @Description: push模式获取消息
 * @Date 2019/6/6 0006
 */
public class MyConsumerForPush {
    public static void main(String[] args) throws Exception{
        //配置消费这的group，这里的group和生产者的group是没有关系的，只是用来标识
        //同一个group下的不同consumer， 不同的group都可消费到同一个topic下的消息
        //这个特性可以用来做广播操作
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("yyl_group_name");
        //设置namesrv的地址
        consumer.setNamesrvAddr("192.168.111.128:9876;192.168.111.129:9876");
        //设置消息消费模式，rocketmq有两种消息消费模式
        //1）CLUSTERING：集群消费，从而达到负载均衡的目的，rocketmq默认是采用这种模式
        //在这种模式下是支持消息失败重投的
        //2）BROADCASTING：广播模式，每个consumer会消费topic下的所有消息
        //在这种模式下是不支持消息失败重投的，所以实际情况中尽量不要使用广播模式
        //如果业务上必须使用广播模式，可创建多个consumer实例，消费同一个topic，但是指定不同的group
        consumer.setMessageModel(MessageModel.CLUSTERING);
        //设置rocketmq的启动消费的位置
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置消费的topic和topic下消息的tag
        consumer.subscribe("yyl_topic","yyl_tag");
        //注册消息消费监听
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try{
                    System.out.println("receive msg :"+msgs);
                    //消息消费都是需要做幂等处理的，因为messageId有可能出现冲突的情况，所以真正幂等的
                    //处理不建议使用messageId作为处理依据，最好的方式是以业务方唯一标识作为幂等处理的关键依据
                    //而业务方的唯一标识可以通过key设置
                    String keys = msgs.get(0).getKeys();
                    //消息消费成功
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }catch (Exception e){
                    //消息消费失败，进行消息重投，也就是进入 %RETRY%topic 队列 也就是 %RETRY%yyl_topic 队列中
                    //默认会重试16次（重试时间间隔递增），如果还失败，将会进入死信队列
                    //注意这种失败重投只在集群模式（CLUSTERING）模式下有效
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });
        consumer.start();
    }
}
