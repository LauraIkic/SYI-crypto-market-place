package at.ikic.cryptoMarketPlace.service;

import at.ikic.cryptoMarketPlace.entity.Coin;
import at.ikic.cryptoMarketPlace.kafka.producer.CoinProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Service
public class CoinService {

    private static final String COIN_GECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd";

    @Autowired
    private CoinProducer coinProducer;

    @Scheduled(fixedRate = 60000)
    public void fetchCoinData() {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<List<Coin>> response = restTemplate.exchange(
                COIN_GECKO_API_URL,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {
                }
        );

        List<Coin> coins = response.getBody();
        coinProducer.sendCoinsToKafka(coins);
    }
}
