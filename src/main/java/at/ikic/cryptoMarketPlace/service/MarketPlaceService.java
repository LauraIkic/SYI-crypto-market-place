package at.ikic.cryptoMarketPlace.service;

import at.ikic.cryptoMarketPlace.entity.Marketplace;
import at.ikic.cryptoMarketPlace.entity.Order;
import at.ikic.cryptoMarketPlace.enums.TransactionStatus;
import at.ikic.cryptoMarketPlace.enums.TransactionType;
import at.ikic.cryptoMarketPlace.kafka.producer.OrderResponseProducer;
import at.ikic.cryptoMarketPlace.repository.MarketPlaceRepository;
import at.ikic.cryptoMarketPlace.repository.OrderRepository;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MarketPlaceService {
    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private MarketPlaceRepository marketplaceRepository;

    @Autowired
    private OrderResponseProducer orderResponseProducer;

    public Marketplace getOrCreateMarketplace() {
        return marketplaceRepository.findFirstByOrderByIdAsc()
                .orElseGet(() -> marketplaceRepository.save(new Marketplace()));
    }

    @Transactional
    public void addOrderToMarketPlace(Order order) {
        Marketplace marketplace = this.getOrCreateMarketplace();

        if (marketplace.getOrders() == null) {
            marketplace.setOrders(new ArrayList<>());
        }

        marketplace.getOrders().add(order);
        marketplaceRepository.save(marketplace);
    }

    public Order matchOrder(Order order) {
        Marketplace marketplace = this.getOrCreateMarketplace();
        List<Order> orderList = marketplace.getOrders();

        return findMatchingOrder(order, orderList);
    }

    public void processTransaction(Order matchingOrder, Order order) {
        if (matchingOrder == null || order == null) return;
        Marketplace marketplace = this.getOrCreateMarketplace();

        marketplace.getOrders().remove(matchingOrder);

        marketplaceRepository.save(marketplace);

        matchingOrder.setStatus(TransactionStatus.CLOSED);
        order.setStatus(TransactionStatus.CLOSED);

        //processed order
        orderResponseProducer.sendOrderToKafka(order);
    }

    /**
     * TODO: turn into sql query
     */
    private Order findMatchingOrder(Order order, List<Order> orderList) {
        TransactionType oppositeType = (order.getType() == TransactionType.BUY)
                ? TransactionType.SELL : TransactionType.BUY;

        for (Order o : orderList) {
            if (o.getType() == oppositeType && o.getPrice() == order.getPrice() && o.getCoinId() == order.getCoinId()) {
                return o;
            }
        }
        return null;
    }
}
