package com.wwj.kafkalearn;

import com.wwj.kafkalearn.message.ConsumerServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;

/**
 * @Author: Mr.Thomas
 * @Date: 2020/10/22 002214:51
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {KafkaLearnApplication.class})
@Slf4j
public class ConsumerTest {

    @Test
    public void ConsumerMsg() {
        KafkaConsumer<String, String> kafkaConsumer = ConsumerServer.getInstance().getKafkaConsumer("topic", null);
        while (true) {
            //1秒监听一次
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            records.forEach(o -> {
                String msg = o.value();
                log.info("msg:{}", msg);
            });
        }
    }
}
