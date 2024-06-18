package best.of.kafka.streams.dto;

public class Cart {

    private String productName;
    private String country;

    public String getCountry() {
        return country;
    }

    public Cart(String productName, String country) {
        this.productName = productName;
        this.country = country;
    }
}
