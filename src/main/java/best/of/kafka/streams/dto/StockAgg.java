package best.of.kafka.streams.dto;

public class StockAgg {
    private String companyName;
    private Double avgPrice;
    private Double sumAllPrices;
    private Integer numberOfHits;
    private Double totalIncome;

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public Double getAvgPrice() {
        return avgPrice;
    }

    public void setAvgPrice(Double avgPrice) {
        this.avgPrice = avgPrice;
    }

    public Double getSumAllPrices() {
        return sumAllPrices;
    }

    public void setSumAllPrices(Double sumAllPrices) {
        this.sumAllPrices = sumAllPrices;
    }

    public Integer getNumberOfHits() {
        return numberOfHits;
    }

    public void setNumberOfHits(Integer numberOfHits) {
        this.numberOfHits = numberOfHits;
    }

    public StockAgg() {
        this.avgPrice = 0d;
        this.numberOfHits = 0;
        this.sumAllPrices = 0d;
        this.totalIncome = 0d;
    }

    public static StockAgg calcAvg(String key, Stock value, StockAgg stockAgg){
        stockAgg.numberOfHits++;
        stockAgg.sumAllPrices += value.getPrice();
        stockAgg.avgPrice = stockAgg.getSumAllPrices()/ stockAgg.numberOfHits;
        return stockAgg;
    }

    public static StockAgg addToTotal(String key, Stock value, StockAgg stockAgg){
        stockAgg.totalIncome += value.getPrice();
        return stockAgg;
    }

    public static StockAgg subtractFromTotal(String key, Stock value, StockAgg stockAgg){
        stockAgg.totalIncome -= value.getPrice();
        return stockAgg;
    }
}
