package best.of.kafka.streams.dto;

import java.util.List;

public class DetailedJob {

    private String companyName;
    private String id;
    private String title;
    private List<String> technologies;
    private Boolean isKafkaStreamNeeded;

    public String getCompanyName() {
        return companyName;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getTechnologies() {
        return technologies;
    }

    public DetailedJob(Job job) {
        this.companyName = job.getCompanyName();
        this.title = job.getTitle();
        this.technologies = job.getTechnologies();
        this.isKafkaStreamNeeded = job.getTechnologies().contains("Kafka streams");
    }

    public DetailedJob(String id, Job job) {
        this.companyName = job.getCompanyName();
        this.title = job.getTitle();
        this.technologies = job.getTechnologies();
        this.isKafkaStreamNeeded = job.getTechnologies().contains("Kafka streams");
        this.id = id;
    }
}
