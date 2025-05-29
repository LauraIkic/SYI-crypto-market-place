package at.ikic.cryptoMarketPlace.kafka.consumer;

import at.ikic.cryptoMarketPlace.constants.KafkaConstant;
import at.ikic.cryptoMarketPlace.entity.Order;
import at.ikic.cryptoMarketPlace.kafka.producer.OrderResponseProducer;
import at.ikic.cryptoMarketPlace.service.MarketPlaceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderRequestConsumer {

    @Autowired
    private MarketPlaceService marketPlaceService;

    @Autowired
    private OrderResponseProducer orderResponseProducer;

    @KafkaListener(topics = KafkaConstant.ORDER_TOPIC, groupId = KafkaConstant.CRYPTO_GROUP, containerFactory = "kafkaListenerContainerFactory")
    public void consume(Order order) {
        /**
         * fix persisting problems
         */
        Order receivedOrder = new Order();
        receivedOrder.setType(order.getType());
        receivedOrder.setUserId(order.getUserId());
        receivedOrder.setPrice(order.getPrice());
        receivedOrder.setQuantity(order.getQuantity());
        receivedOrder.setStatus(order.getStatus());
        receivedOrder.setVersion(order.getVersion() + 1);
        receivedOrder.setReferenceId(order.getId());

        Order matchingOrder = marketPlaceService.matchOrder(receivedOrder);

        if (matchingOrder != null) {
            marketPlaceService.processTransaction(matchingOrder, receivedOrder);
        } else {
            marketPlaceService.addOrderToMarketPlace(receivedOrder);
        }
    }
}
