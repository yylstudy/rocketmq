package com.yyl.test01;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Author yang.yonglian
 * @ClassName: com.yyl.test01
 * @Description: 同步和异步生产消息
 * @Date 2019/6/6 0006
 */
public class MyProducer {
    public static void main(String[] args) throws Exception{
        //指定group名称，用于关联group上的所有的Producer
        DefaultMQProducer producer = new DefaultMQProducer("yyl_group_name");
        //当一个jvm需要启动多个producer时，通过设置不同的instanceName来区分，不设置的话默认为"DEFAULT"
        producer.setInstanceName("instance1");
        //设置发送失败消息重试次数
        producer.setRetryTimesWhenSendFailed(3);
        //设置namesrv的地址
        producer.setNamesrvAddr("192.168.111.128:9876;192.168.111.129:9876");

        producer.start();
        //同步发送消息，
        for(int i=0;i<10;i++){
            //创建一个消息，可以指定topic的名称 和 tag名称
            //如果抛出 No route info of this topic 异常，有可能是rocketmq的版本和rocket-client的jar包版本不一致导致的
            Message message = new Message("yyl_topic",
                    "yyl_tag","hello rocketmq".getBytes(RemotingHelper.DEFAULT_CHARSET));
            //设置消息的key，这个一般用作消息消费时的幂等判断
            message.setKeys("keys"+i);
            //同步发送消息，这个会在接收到broker响应后才发送下一条消息的通信方式
            //这个通常用于比较重要的消息，部分顺序消息的实现也是采用同步的方式
            SendResult result = producer.send(message);
            System.out.println(result);
        }
        //异步发送消息
        for(int i=0;i<10;i++){
            Message message = new Message("yyl_topic",
                    "yyl_tag","helle rocketmq".getBytes(RemotingHelper.DEFAULT_CHARSET));
            //设置消息延迟发送的等级（1s/5s/10s/30s/1m/2m/3m）等
            message.setDelayTimeLevel(3);
            //消息发送回调
            producer.send(message, new SendCallback() {
                //消息发送成功回调
                @Override
                public void onSuccess(SendResult sendResult) {
                    //有四种发送状态
                    //1)FLUSH_DISK_TIMEOUT:刷盘超时，这个需要broker的刷盘策略被设置成SYNC_FLUSH才会报这个错误
                    //2)FLUSH_SLAVE_TIMEOUT：主从同步超时，这个需要在主备方式下，并且broker被设置成SYNC_MASTER才会报这个错误
                    //3)SLAVE_NOT_AVAILABLE:未找到Slave的broker,这个需要在主备的方式下，并且broker被设置成SYNC_MASTER才会报错
                    //4)SEND_OK:消息发送成功，
                    System.out.println("message send status "+sendResult);
                }
                //消息发送失败回调
                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
        }
        Thread.sleep(100000);
        producer.shutdown();
    }
}
