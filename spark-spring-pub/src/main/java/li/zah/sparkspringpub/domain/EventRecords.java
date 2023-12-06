package li.zah.sparkspringpub.domain;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.Data;

@Data
public class EventRecords implements Serializable {

  private LocalDate simulationStart;

  private Set<EmployeeType> operatorTypes = new LinkedHashSet<>();

  private Set<Facility> facilities = new LinkedHashSet<>();

  private List<List<String>> events = new ArrayList<>();

}
