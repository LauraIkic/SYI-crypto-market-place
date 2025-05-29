package at.ikic.cryptoMarketPlace.dto.request;

import at.ikic.cryptoMarketPlace.enums.TransactionType;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class OrderCreateDto {
    @NotNull(message = "Transaction type can not be null")
    private TransactionType type;

    @NotNull(message = "Coin Id can not be null")
    private String coinId;

    @NotNull(message = "Quantity can not be null")
    @Min(value = 1, message = "Quantity must be greater than 0")
    private double quantity;
}
