package at.ikic.cryptoMarketPlace.kafka.consumer;

import at.ikic.cryptoMarketPlace.constants.KafkaConstant;
import at.ikic.cryptoMarketPlace.entity.Order;
import at.ikic.cryptoMarketPlace.enums.TransactionStatus; // KORRIGIERT: enums statt entity!
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
        System.out.println("=== ORDER EMPFANGEN VON KAFKA ===");
        System.out.println("Order ID: " + order.getId());
        System.out.println("User ID: " + order.getUserId());
        System.out.println("Coin: " + order.getCoinId());
        System.out.println("Type: " + order.getType());
        System.out.println("Quantity: " + order.getQuantity());
        System.out.println("Price: " + order.getPrice());

        /**
         * fix persisting problems
         */
        Order receivedOrder = new Order();
        receivedOrder.setType(order.getType());
        receivedOrder.setUserId(order.getUserId());
        receivedOrder.setCoinId(order.getCoinId());
        receivedOrder.setPrice(order.getPrice());
        receivedOrder.setQuantity(order.getQuantity());
        receivedOrder.setStatus(TransactionStatus.OPEN);
        receivedOrder.setVersion(order.getVersion() != null ? order.getVersion() + 1 : 1L);
        receivedOrder.setReferenceId(order.getId());

        System.out.println("Suche nach Matching Order...");
        Order matchingOrder = marketPlaceService.matchOrder(receivedOrder);

        if (matchingOrder != null) {
            System.out.println("Match gefunden! Verarbeite Transaktion...");
            marketPlaceService.processTransaction(matchingOrder, receivedOrder);
        } else {
            System.out.println("Kein Match gefunden. Füge Order ins Orderbuch ein...");
            marketPlaceService.addOrderToMarketPlace(receivedOrder);
            
            // IMMER Response senden, auch ohne Match
            receivedOrder.setStatus(TransactionStatus.EXECUTED);
            System.out.println("Sende Order-Response zurück: EXECUTED");
            orderResponseProducer.sendOrderToKafka(receivedOrder);
        }
    }
}
