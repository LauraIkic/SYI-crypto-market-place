package at.ikic.cryptoMarketPlace.kafka.producer;

import at.ikic.cryptoMarketPlace.constants.KafkaConstant;
import at.ikic.cryptoMarketPlace.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderResponseProducer {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    public void sendOrderToKafka(Order order) {
        kafkaTemplate.send(KafkaConstant.ORDER_RESPONSE_TOPIC, order);
    }
}
