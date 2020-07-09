package com.yyl.test01;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author yang.yonglian
 * @ClassName: com.yyl
 * @Description: pull方式的消费
 * @Date 2019/6/4 0004
 */
public class MyConsumerForPull {
    private static final Map<MessageQueue,Long> OFFSET_TABLE = new HashMap();
    public static void main(String[] args) throws Exception{
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name");
        consumer.setNamesrvAddr("192.168.216.145:9876;192.168.216.148:9876");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.start();
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("topicTest");
        for(MessageQueue mq:mqs){
            long offset = consumer.fetchConsumeOffset(mq,true);
            SINGLE_MQ:
            while(true){
                PullResult pullResult = consumer.
                        pullBlockIfNotFound(mq,null,getMessageQueueOffset(mq),32);
                System.out.println(pullResult);
                System.out.println(pullResult.getNextBeginOffset());
                putMessageQueueOffset(mq,pullResult.getNextBeginOffset());
                switch(pullResult.getPullStatus()){
                    case FOUND:
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                        break SINGLE_MQ;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }

            }
        }
        consumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq){
        Long offset = OFFSET_TABLE.get(mq);
        if(offset!=null){
            return offset;
        }
        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq,long offset){
        OFFSET_TABLE.put(mq,offset);
    }

}
