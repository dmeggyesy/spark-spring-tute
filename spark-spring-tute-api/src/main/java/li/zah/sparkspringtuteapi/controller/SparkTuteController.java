package li.zah.sparkspringtuteapi.controller;


import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEES;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_ATTRITION;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_ATTRITION_DATES;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_ATTRITION_TIMELINE;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_ATTRITION_TIMELINE_AVG;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_COUNT;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_COUNT_OLD;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_WORKFORCE;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.EMPLOYEE_WORKFORCE_TIMELINE_AVG;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.RECORD;
import static li.zah.sparkspringtuteapi.config.ApiEndpoint.TEST;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import li.zah.sparkspringpub.domain.EventRecord;
import li.zah.sparkspringpub.domain.EventRecords;
import li.zah.sparkspringtuteapi.service.SparkTuteService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SparkTuteController {

  @NonNull
  private final SparkTuteService sparkTuteService;

  @RequestMapping(value = { TEST }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody String getApi() {
    log.info("Test endpoint");
    return sparkTuteService.test();
  }

  @PostMapping(value = { RECORD }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody List<EventRecord> processRecord(@RequestBody EventRecords record,
      @RequestParam(required = true) String empId) {

    log.info("Processing Record");

    return sparkTuteService.filterByEmpId(record, empId);
  }

  @PostMapping(value = { EMPLOYEES }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Map<String, List<EventRecord>> generateEmployees(@RequestBody EventRecords record) {

    log.info("Processing Employees");

    return sparkTuteService.getAllEventsByEmployees(record);
  }

  @PostMapping(value = { EMPLOYEE_COUNT }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Long getEmployeeCount(@RequestBody EventRecords record,
      @RequestParam(required = true) @DateTimeFormat(iso = ISO.DATE) LocalDate date) {

    log.info("Number of active Employees");

    return sparkTuteService.getNumberOfEmployeesAtTimestep(record, date);
  }

  @PostMapping(value = { EMPLOYEE_COUNT_OLD }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Long getEmployeeCountOld(@RequestBody EventRecords record,
      @RequestParam(required = true) @DateTimeFormat(iso = ISO.DATE) LocalDate date) {

    log.info("Number of active Employees");

    return sparkTuteService.getNumberOfEmployeesAtTimestepOld(record, date);
  }

  @PostMapping(value = { EMPLOYEE_ATTRITION }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Long getAttritionRange(@RequestBody EventRecords record,
      @RequestParam(required = true) @DateTimeFormat(iso = ISO.DATE) LocalDate rangeStart,
      @RequestParam(required = true) @DateTimeFormat(iso = ISO.DATE) LocalDate rangeEnd) {

    log.info("Number of active Employees");

    return sparkTuteService.attritionInRange(record, rangeStart, rangeEnd);
  }

  @PostMapping(value = { EMPLOYEE_ATTRITION_DATES }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Map<String, LocalDate> getAttritionDates(@RequestBody EventRecords record) {

    log.info("Number of active Employees");

    return sparkTuteService.attritionDays(record);
  }

  @PostMapping(value = { EMPLOYEE_ATTRITION_TIMELINE }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Map<LocalDate, Long> getAttritionTimeline(@RequestBody EventRecords record) {

    log.info("Number of active Employees");

    return sparkTuteService.attritionTimelineOld(record);
  }

  @PostMapping(value = { EMPLOYEE_ATTRITION_TIMELINE_AVG }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Map<LocalDate, BigDecimal> getAttritionTimelineAvg(@RequestBody List<EventRecords> record) {

    log.info("Number of active Employees");

    return sparkTuteService.attritionTimelineAvg(record);
  }

  @PostMapping(value = { EMPLOYEE_WORKFORCE }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Map<LocalDate, Long> getWorkforceTimeline(@RequestBody List<EventRecords> record) {

    log.info("Number of active WF");

    return sparkTuteService.workforceTimelineSingle(record);
  }

  @PostMapping(value = { EMPLOYEE_WORKFORCE_TIMELINE_AVG }, produces = MediaType.APPLICATION_JSON_VALUE)
  public @ResponseBody Map<LocalDate, BigDecimal> getWorkforceTimelineAvg(@RequestBody List<EventRecords> record) {

    log.info("Number of active WF avg");

    return sparkTuteService.workforceTimelineAvg(record);
  }

}
