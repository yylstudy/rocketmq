package com.yyl.test02;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @Author yang.yonglian
 * @ClassName: com.yyl.test02
 * @Description: 顺序消息：分为全局顺序消息和部分顺序消息，rocketmq默认情况下是8个读队列和8个写队列
 * 1）全局顺序消息：是指某个topic下所有消息都要保证顺序性，要保证全局顺序消息，
 *    需要把topic的读写队列设置为1，producer和consumer的并发也设置为1，实际用的少
 * 2）部分顺序消息：保证每一组消息被顺序消费即可 (订单的生成、付款、发货)这三个消息必须按照顺序处理才行
 * @Date 2019/6/7 0007
 */
public class MyProducerForOrder {
    public static void main(String[] args) throws Exception{
        //部分顺序消息
        DefaultMQProducer producer = new DefaultMQProducer("order_group");
        producer.setNamesrvAddr("192.168.111.128:9876;192.168.111.129:9876");
        producer.start();
        for(int  i=0;i<10;i++){
            Message message = new Message("order_topic","yyl_tag",("hello rocketmq"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //注意这里需要采用同步的方式发送，注意两个arg参数是对应的
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    System.out.println("要发送队列的长度为:"+mqs.size());
                    System.out.println("要发送的消息为："+msg);
                    //order1、order4、order7发送到同一队列
                    if("order1".equals(arg)||"order4".equals(arg)||"order7".equals(arg)){
                        return mqs.get(0);
                    }
                    //order2、order5、order8发送到同一队列
                    else if("order2".equals(arg)||"order5".equals(arg)||"order8".equals(arg)){
                        return mqs.get(1);
                    }
                    ////order3、order6、order9发送到同一队列
                    else if("order3".equals(arg)||"order6".equals(arg)||"order9".equals(arg)){
                        return mqs.get(2);
                    }else{
                        return mqs.get(3);
                    }
                }
            }, "order"+i);
            System.out.println(sendResult);
        }
    }
}
