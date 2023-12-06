package li.zah.sparkspringtuteapi.config;

public class ApiEndpoint {
  public final static String BASE = "/api";

  public final static String TEST = BASE + "/test";

  public final static String RECORD = BASE + "/record";

  public final static String EMPLOYEES = RECORD + "/employees";

  public final static String EMPLOYEE_COUNT = EMPLOYEES + "/active";

  public final static String EMPLOYEE_ATTRITION = EMPLOYEES + "/attrition";

  public final static String EMPLOYEE_ATTRITION_DATES = EMPLOYEE_ATTRITION + "/dates";

  public final static String EMPLOYEE_ATTRITION_TIMELINE = EMPLOYEE_ATTRITION + "/timeline";

  public final static String EMPLOYEE_WORKFORCE = EMPLOYEES + "/workforce";

  public final static String EMPLOYEE_WORKFORCE_TIMELINE_AVG = EMPLOYEE_WORKFORCE + "/avg";

  public final static String EMPLOYEE_ATTRITION_TIMELINE_AVG = EMPLOYEE_ATTRITION_TIMELINE + "/avg";

  public final static String EMPLOYEE_COUNT_OLD = EMPLOYEES + "/active-old";

}
