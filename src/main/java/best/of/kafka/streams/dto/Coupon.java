package best.of.kafka.streams.dto;

public class Coupon {

    private Integer discount;

    public Integer getDiscount() {
        return discount;
    }

    public Coupon(Integer discount) {
        this.discount = discount;
    }
}
