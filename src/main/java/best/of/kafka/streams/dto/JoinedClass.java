package best.of.kafka.streams.dto;

public class JoinedClass {

    private String name;
    private Integer discount;

    private Integer price;

    private Integer priceAfterDiscount;

    public Integer getPrice() {
        return price;
    }

    public Integer getPriceAfterDiscount() {
        return priceAfterDiscount;
    }

    public Integer getDiscount() {
        return discount;
    }

    public String getName() {
        return name;
    }

    public JoinedClass(String name, Integer discount, Integer price, Integer priceAfterDiscount) {
        this.name = name;
        this.discount = discount;
        this.price = price;
        this.priceAfterDiscount = priceAfterDiscount;
    }

    public JoinedClass(String name, Integer discount) {
        this.name = name;
        this.discount = discount;
    }

    public JoinedClass(Product product, Coupon coupon) {
        this.name = product.getName();
        this.price = product.getPrice();
        this.discount =  coupon == null ? null :coupon.getDiscount();
        this.priceAfterDiscount = coupon == null || product.getPrice() == null ? null : product.getPrice() * coupon.getDiscount()/100;
    }

    public JoinedClass(String name, Product product, Coupon coupon) {
        this.name = name;
        this.price = product.getPrice();
        this.discount =  coupon == null ? null :coupon.getDiscount();
        this.priceAfterDiscount = coupon == null || product.getPrice() == null ? null : product.getPrice() * coupon.getDiscount()/100;
    }
}