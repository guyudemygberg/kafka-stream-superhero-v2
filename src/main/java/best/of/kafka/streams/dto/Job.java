package best.of.kafka.streams.dto;

import java.util.List;

public class Job {

    private String companyName;
    private String title;
    private List<String> technologies;

    public String getCompanyName() {
        return companyName;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getTechnologies() {
        return technologies;
    }
}
