package com.wwj.kafkalearn;

import com.wwj.kafkalearn.message.ProducerServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @Author: Mr.Thomas
 * @Date: 2020/10/22 002214:18
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {KafkaLearnApplication.class})
@Slf4j
public class ProducerTest {

    @Autowired
    private ProducerServer producerServer;

    @Test
    public void sendMsgTest() {
        try {
            producerServer.sendMsg("topic", "kafka-demo", "hello,kafka...Hi...",false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
