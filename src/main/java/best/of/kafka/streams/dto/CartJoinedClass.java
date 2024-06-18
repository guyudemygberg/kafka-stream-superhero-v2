package best.of.kafka.streams.dto;

public class CartJoinedClass {

    private String key;
    private String productName;
    private Integer price;
    private Integer discount;
    private Integer priceAfterDiscount;
    private String country;

    public CartJoinedClass(JoinedClass joinedClass, Cart cart) {
        this.productName = joinedClass.getName();
        this.price = joinedClass.getPrice();
        this.discount = joinedClass.getDiscount();
        this.priceAfterDiscount = joinedClass.getPriceAfterDiscount();
        this.country = cart == null ? null : cart.getCountry();
    }

    public CartJoinedClass(String key, JoinedClass joinedClass, Cart cart) {
        this.key = key;
        this.productName = joinedClass.getName();
        this.price = joinedClass.getPrice();
        this.discount = joinedClass.getDiscount();
        this.priceAfterDiscount = joinedClass.getPriceAfterDiscount();
        this.country = cart == null ? null : cart.getCountry();
    }
}
