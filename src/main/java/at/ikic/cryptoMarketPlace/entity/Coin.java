package at.ikic.cryptoMarketPlace.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Entity
@Data
public class Coin implements Serializable {

    @Id
    @JsonProperty("id")
    private String id;

    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("name")
    private String name;

    @JsonProperty("image")
    private String image;

    @Column(name = "current_price")
    @JsonProperty("current_price")
    private double currentPrice;

    @Column(name = "market_cap")
    @JsonProperty("market_cap")
    private long marketCap;

    @Column(name = "market_cap_rank")
    @JsonProperty("market_cap_rank")
    private int marketCapRank;

    @Column(name = "fully_diluted_valuation")
    @JsonProperty("fully_diluted_valuation")
    private long fullyDilutedValuation;

    @Column(name = "total_volume")
    @JsonProperty("total_volume")
    private long totalVolume;

    @Column(name = "high_24h")
    @JsonProperty("high_24h")
    private double high24h;

    @Column(name = "low_24h")
    @JsonProperty("low_24h")
    private double low24h;

    @Column(name = "price_change_24h")
    @JsonProperty("price_change_24h")
    private double priceChange24h;

    @Column(name = "price_change_percentage_24h")
    @JsonProperty("price_change_percentage_24h")
    private double priceChangePercentage24h;

    @Column(name = "market_cap_change_24h")
    @JsonProperty("market_cap_change_24h")
    private long marketCapChange24h;

    @Column(name = "market_cap_change_percentage_24h")
    @JsonProperty("market_cap_change_percentage_24h")
    private double marketCapChangePercentage24h;

    @Column(name = "circulating_supply")
    @JsonProperty("circulating_supply")
    private long circulatingSupply;

    @Column(name = "total_supply")
    @JsonProperty("total_supply")
    private long totalSupply;

    @Column(name = "max_supply")
    @JsonProperty("max_supply")
    private long maxSupply;

    @Column(name = "ath")
    @JsonProperty("ath")
    private double ath;

    @Column(name = "ath_change_percentage")
    @JsonProperty("ath_change_percentage")
    private double athChangePercentage;

    @Temporal(TemporalType.TIMESTAMP)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    @Column(name = "ath_date")
    @JsonProperty("ath_date")
    private Date athDate;

    @Column(name = "atl")
    @JsonProperty("atl")
    private double atl;

    @Column(name = "atl_change_percentage")
    @JsonProperty("atl_change_percentage")
    private double atlChangePercentage;

    @Temporal(TemporalType.TIMESTAMP)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    @Column(name = "atl_date")
    @JsonProperty("atl_date")
    private Date atlDate;

    // If you decide to use a Map instead of a specific field for ROI:
  /*  @Column(name = "roi")
    @JsonProperty("roi")
    private Map<String, Object> roi;*/

    @Temporal(TemporalType.TIMESTAMP)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    @Column(name = "last_updated")
    @JsonProperty("last_updated")
    private Date lastUpdated;
}
