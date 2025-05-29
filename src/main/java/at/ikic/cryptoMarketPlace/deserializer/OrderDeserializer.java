package at.ikic.cryptoMarketPlace.deserializer;

import at.ikic.cryptoMarketPlace.entity.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;


public class OrderDeserializer implements Deserializer<Order> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Order deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, objectMapper.getTypeFactory().constructType(Order.class));
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Order", e);
        }
    }

    @Override
    public void close() {
        // No resource cleanup required
    }
}
