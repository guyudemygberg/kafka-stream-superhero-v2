package best.of.kafka.streams.dto;

public class Product {

    private String name;
    private Integer price;

    public Integer getPrice() {
        return price;
    }

    public String getName() {
        return name;
    }

    public Product(String name) {
        this.name = name;
    }

    public Product(String name, Integer price) {
        this.name = name;
        this.price = price;
    }
}
