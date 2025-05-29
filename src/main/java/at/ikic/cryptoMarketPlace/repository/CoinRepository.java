package at.ikic.cryptoMarketPlace.repository;


import at.ikic.cryptoMarketPlace.entity.Coin;
import org.springframework.data.jpa.repository.JpaRepository;


public interface CoinRepository extends JpaRepository <Coin, String>{

}
