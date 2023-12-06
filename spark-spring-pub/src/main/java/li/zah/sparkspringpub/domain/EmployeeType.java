package li.zah.sparkspringpub.domain;

import java.io.Serializable;
import lombok.Data;

@Data
public class EmployeeType implements Serializable {

  private Integer id;

  private String name;

  private Integer organisation;

  private String organisationName;

}
