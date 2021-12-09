package com.wwj.kafkalearn.message;

import com.wwj.kafkalearn.interceptor.PrefixProducerInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @Author: Mr.Thomas
 * @Date: 2020/10/22 002213:51
 * doc:KafkaProducer是线程安全的，多个线程间可以共享使用同一个KafkaProducer
 */
@Service
@Slf4j
public class ProducerServer {

    private KafkaProducer<String, String> kafkaProducer;

    @PostConstruct
    public void init() {
        Properties properties = new Properties();
        //设置key的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //设置值序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        //设置集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //设置自定义拦截器
//        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PrefixProducerInterceptor.class.getName());
//        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_DOC, "消息加前缀");

        //ack=0,生产者在成功写入消息之前不会等待任何来自服务器的相应，如果出现问题生产者是感知不到的，消息就丢了。不过因为生产者不需要等待服务器相应，所以它可以以网络能够支持的最大速度发送消息，从而达到很高的吞吐量；
        //ack=1,默认值为1，只要master节点收到消息，生产者就会收到一个来自服务器的成功相应。如果消息无法达到master节点(master节点崩溃，新的master节点还没有被选举出来),生产者会收到一个错误的相应，为了避免数据丢失，生产者会重新发送消息。但是还是可能会导致数据丢失，如果收到写成功通知，此时master节点还没来得及同步数据到follower节点，master节点崩溃，就会导致数据丢失；
        //ack=-1,只有当所有参与复制的节点都收到消息时，生产者会收到一个来自服务器的成功相应，这种模式是最安全的，他可以保证不止一个服务器收到消息；
        //设置ACK【acks参数配置的是个字符串类型，而不是整数类型，配置为整数类型会抛出异常：org.apache.kafka.common.config.ConfigException: Invalid value 0 for configuration acks: Expected value to be a string, but it was a java.lang.Integer】
        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        //KafkaProducer是线程安全的，多个线程间可以共享使用同一个KafkaProducer
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    /**
     * @param topic
     * @param key
     * @param msg
     * @param isASyncSend true:同步发送   false：异步发送
     */
    public void sendMsg(String topic, String key, String msg, Boolean isASyncSend) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);
        try {
            if (isASyncSend) {
                //flag为true， 同步发送，会有一个Future<RecordMetadata>对象，不报异常，调用get()方法会得到topic、partition、offset
                Future<RecordMetadata> future = kafkaProducer.send(record);
                log.info("SUCCESS flag:{},topic:{},partition:{},msg:{}", isASyncSend, record.topic(), record.partition(), record.value());
            } else {
                //异步发送
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            log.error("FAIL flag:{},topic:{},partition:{},msg:{},errorMsg:{}", isASyncSend, record.topic(), record.partition(), record.value(), e.getMessage());
                        } else {
                            log.info("SUCCESS flag:{},topic:{},partition:{},msg:{}", isASyncSend, record.topic(), record.partition(), record.value());
                        }
                    }
                });
            }
            kafkaProducer.close();
        } catch (Exception e) {
            log.error("FAIL flag:{},topic:{},partition:{},msg:{}", isASyncSend, record.topic(), record.partition(), record.value());
        }
    }
}
