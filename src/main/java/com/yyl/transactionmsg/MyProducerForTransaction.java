package com.yyl.transactionmsg;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Author yang.yonglian
 * @ClassName: com.yyl.transactionmsg
 * @Description: TODO(这里描述)
 * @Date 2019/6/5 0005
 */
public class MyProducerForTransaction {
    public static void main(String[] args) throws Exception{
        MyProducerForTransaction transaction = new MyProducerForTransaction();
        transaction.createOrder();
    }

    /**
     * 创建订单接口，这个方法需要添加事务
     */
    public void createOrder() throws Exception{
        //发送消息
        sendMsg();
    }

    private void sendMsg() throws Exception{
        TransactionMQProducer producer = new TransactionMQProducer("transaction_group");
        //创建一个事务监听器
        OrderTransactionListenerImpl listener = new OrderTransactionListenerImpl();
        producer.setTransactionListener(listener);
        producer.setNamesrvAddr("192.168.111.1228:9876;192.168.111.129:9876");
        producer.start();
        Order order = new Order("1","order1");
        Message message = new Message("transaction_topic","yyl_tag",
                JSON.toJSONString(order).getBytes(RemotingHelper.DEFAULT_CHARSET));
        SendResult result = producer.sendMessageInTransaction(message,null);
        System.out.println(result);
    }

    static class OrderTransactionListenerImpl implements TransactionListener{
        /**
         * 执行本地事务，根据本地事务的执行情况
         * @param message
         * @param o
         * @return
         */
        @Override
        public LocalTransactionState executeLocalTransaction(Message message, Object o) {
            String msg = new String(message.getBody());
            try{
                //将接收到的订单入库，如果订单不是幂等的，可以使用消息key
                doBusinessCommit(JSON.parseObject(msg,Order.class));
            }catch (Exception e){
                e.printStackTrace();
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
            return LocalTransactionState.COMMIT_MESSAGE;
        }

        /**
         * 查询本地消息是否存在
         * @param messageExt
         * @return
         */
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
            //使用消息key进行本地库查询
            boolean isExists = checkBusinessStatus(messageExt.getKeys());
            if(isExists){
                return LocalTransactionState.COMMIT_MESSAGE;
            }else{
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }

        /**
         * 查询订单是否存在
         * @param messageKey
         * @return
         */
        private boolean checkBusinessStatus(String messageKey){
            if(true){
                System.out.println("查询数据库 messageKey为"+messageKey+"的消息已经消费成功了，可以提交消息");
                return true;
            }else{
                System.out.println("查询数据库 messageKey为"+messageKey+"的消息不存在或者未消费成功了，可以回滚消息");
                return false;
            }
        }

        /**
         * 订单入库
         * @param order
         * @return
         */
        private Order doBusinessCommit(Order order){
            System.out.println("订单入库成功:"+order);
            return order;
        }
    }

    @Data
    @AllArgsConstructor
    public static class Order{
        private String orderId;
        private String orderName;
    }
}
