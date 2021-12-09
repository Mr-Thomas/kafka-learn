package com.wwj.kafkalearn;

import com.wwj.kafkalearn.message.MsgProducer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author ：Administrator
 * @description：TODO
 * @date ：2021/12/9 14:57
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {KafkaLearnApplication.class})
@Slf4j
public class MsgProducerTest {
    @Autowired
    private MsgProducer msgProducer;

    @Test
    public void sendMsgTest() {
        try {
            msgProducer.sendMessageToKafkaServer("message-模式", "topic", false, "key");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
