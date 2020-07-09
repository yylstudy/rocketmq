package com.yyl.test02;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
        //Producer的groupName和Consumer的groupName必须要不一样，否则消息消费会出现消费不完全或者消费不到的情况
        DefaultMQProducer producer = new DefaultMQProducer("order_group_producer");
        producer.setNamesrvAddr("192.168.216.145:9876;192.168.216.148:9876");
        producer.start();
        List<Order> orders = buildOrders();
        for(Order order:orders){
            Message message = new Message("order_topic","yyl_tag",order.toString()
                    .getBytes(RemotingHelper.DEFAULT_CHARSET));
            message.setKeys(UUID.randomUUID().toString());
            System.out.println(message.getKeys());
            //注意这里需要采用同步的方式发送，注意两个arg参数是对应的
            SendResult sendResult = producer.send(message,(mqs,msg,arg)->{
                int queueIndex = arg.hashCode()%mqs.size();
                System.out.println("队列总数:"+mqs.size());
                System.out.println("发送至队列下标为:"+queueIndex);
                return mqs.get(queueIndex);
            },order.getGroupName());
            if(sendResult.getSendStatus()!= SendStatus.SEND_OK){
                throw new RuntimeException("消息发送失败"+sendResult.getSendStatus());
            }
        }
    }
    private static List<Order> buildOrders(){
        List<Order> orders = new ArrayList<>();
        Order order = new Order("话费充值","查找号码");
        orders.add(order);
        order = new Order("话费充值","查找余额");
        orders.add(order);
        order = new Order("话费充值","付款");
        orders.add(order);
        order = new Order("买空调","余额查询");
        orders.add(order);
        order = new Order("买空调","库存查询");
        orders.add(order);
        order = new Order("买空调","物流查询");
        orders.add(order);
        order = new Order("买空调","付款");
        orders.add(order);
        return orders;
    }
    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class Order{
        private String groupName;
        private String name;
    }
}
