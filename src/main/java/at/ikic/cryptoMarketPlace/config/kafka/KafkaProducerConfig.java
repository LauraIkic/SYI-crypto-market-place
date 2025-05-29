package at.ikic.cryptoMarketPlace.config.kafka;

import at.ikic.cryptoMarketPlace.constants.KafkaConstant;
import at.ikic.cryptoMarketPlace.entity.Coin;
import at.ikic.cryptoMarketPlace.entity.Order;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.List;

@Configuration
@EnableKafka
public class KafkaProducerConfig {

    @Bean
    public NewTopic createTopicCoin() {
        return new NewTopic(KafkaConstant.CRYPTO_COIN_TOPIC,3, (short) 1 );
    }

    @Bean
    public NewTopic createTopicOrder() {
        return new NewTopic(KafkaConstant.ORDER_RESPONSE_TOPIC,3, (short) 1 );
    }


    @Bean
    public KafkaTemplate<String, List<Coin>> kafkaTemplateCoins() {
        return new KafkaTemplate<>(producerFactoryCoin());
    }

    @Bean
    public ProducerFactory<String, List<Coin>> producerFactoryCoin() {
        var producerProps = new java.util.HashMap<String, Object>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    @Bean
    public KafkaTemplate<String, Order> kafkaTemplateOrder() {
        return new KafkaTemplate<>(producerFactoryOrder());
    }

    @Bean
    public ProducerFactory<String, Order> producerFactoryOrder() {
        var producerProps = new java.util.HashMap<String, Object>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(producerProps);
    }
}
