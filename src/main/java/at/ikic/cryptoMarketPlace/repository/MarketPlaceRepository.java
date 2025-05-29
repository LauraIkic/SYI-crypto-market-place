package at.ikic.cryptoMarketPlace.repository;

import at.ikic.cryptoMarketPlace.entity.Marketplace;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface MarketPlaceRepository extends JpaRepository <Marketplace, UUID>{
    Optional<Marketplace> findFirstByOrderByIdAsc();

}
