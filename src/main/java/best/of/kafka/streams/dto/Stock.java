package best.of.kafka.streams.dto;

public class Stock {
    private String companyName;
    private Double price;
    private Integer numberOfHits;
    private Double sumAllPrices;
    private Double totalIncome;

    public Double getTotalIncome() {
        return totalIncome;
    }

    public void setTotalIncome(Double totalIncome) {
        this.totalIncome = totalIncome;
    }

    public Stock(String companyName, Double price) {
        this.companyName = companyName;
        this.price = price;
    }

    public String getCompanyName() {
        return companyName;
    }

    public Integer getNumberOfHits() {
        return numberOfHits;
    }

    public void setNumberOfHits(Integer numberOfHits) {
        this.numberOfHits = numberOfHits;
    }

    public Double getSumAllPrices() {
        return sumAllPrices;
    }

    public void setSumAllPrices(Double sumAllPrices) {
        this.sumAllPrices = sumAllPrices;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Stock addPrice(Stock stock){
        this.numberOfHits = this.numberOfHits == null ? 2 : this.numberOfHits+1;
        this.sumAllPrices = this.sumAllPrices == null ? this.price + stock.getPrice() : this.sumAllPrices + stock.getPrice();
        this.price = this.sumAllPrices/this.numberOfHits;
        return this;
    }
}
