package li.zah.sparkspringpub.domain;

import java.io.Serializable;
import java.util.List;
import lombok.Data;

@Data
public class EventRecord implements Serializable {

  private String sequence;

  private String timestamp;

  private String employeeId;

  private String employeeType;

  private String eventType;

  private String sourceFacility;

  private String targetFacility;

  private String proficiency;

  private String position;

  public EventRecord(String[] args) {

    this.sequence = args[0];
    this.timestamp = args[1];
    this.employeeId = args[2];
    this.employeeType = args[3];
    this.eventType = args[4];
    this.sourceFacility = args[5];
    this.targetFacility = args[6];
    this.proficiency = args[7];
    this.position = args[8];
  }

  public EventRecord(List<String> args) {

    this.sequence = args.get(0);
    this.timestamp = args.get(1);
    this.employeeId = args.get(2);
    this.employeeType = args.get(3);
    this.eventType = args.get(4);
    this.sourceFacility = args.get(5);
    this.targetFacility = args.get(6);
    this.proficiency = args.get(7);
    this.position = args.get(8);
  }

}
