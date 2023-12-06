package li.zah.sparkspringtuteapi.service;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.PostConstruct;
import li.zah.sparkspringpub.SparkSpringProcessor;
import li.zah.sparkspringpub.domain.AverageCombiner;
import li.zah.sparkspringpub.domain.EventRecord;
import li.zah.sparkspringpub.domain.EventRecords;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import com.clearspring.analytics.util.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import org.threeten.extra.Days;
import scala.Tuple2;

@Service
@RequiredArgsConstructor
public class SparkTuteService implements Serializable {

  @NonNull
  private JavaSparkContext sparkContext;

  private SparkSpringProcessor processor;

  @PostConstruct
  public void init() {
    this.processor = new SparkSpringProcessor(sparkContext);
  }

  public String test() {
    return processor.test();
  }

  public List<EventRecord> filterByEmpId(EventRecords record, String empId) {

    return processor.filterByEmpId( record, empId);
  }

  public JavaPairRDD<String, Iterable<EventRecord>> groupedByEmployeeId(JavaRDD<EventRecord> dataset) {
    return processor.groupedByEmployeeId(dataset);
  }

  public JavaRDD<EventRecord> loadEventRecord(EventRecords record) {

    return processor.loadEventRecord( record);

  }

  public Map<String, List<EventRecord>> getAllEventsByEmployees(EventRecords record) {
    return processor.getAllEventsByEmployees( record);
  }

  public Long getNumberOfEmployeesAtTimestep(EventRecords record, LocalDate date) {

    return processor.getNumberOfEmployeesAtTimestep( record, date );
  }

  @Deprecated
  public Long getNumberOfEmployeesAtTimestepOld(EventRecords record, LocalDate date) {

    return processor.getNumberOfEmployeesAtTimestep(record, date);
  }

  public Long attritionInRange(EventRecords record, LocalDate rangeStart, LocalDate rangeEnd) {

    return processor.attritionInRange(record, rangeStart, rangeEnd);
  }

  public Map<String, LocalDate> attritionDays(EventRecords records) {

    return processor.attritionDays(records);
  }

  @Deprecated
  public Map<LocalDate, Long> attritionTimelineOld(EventRecords records) {

    return processor.attritionTimelineOld(records);
  }


  @Deprecated
  public Map<LocalDate, BigDecimal> oldAverageRDDs(List<JavaPairRDD<LocalDate, Long>> datasets) {

    return processor.oldAverageRDDs(datasets);
  }

  @Deprecated
  public Map<LocalDate, BigDecimal> oldAverageRDD2(List<JavaPairRDD<LocalDate, Long>> rddList) {
    return processor.oldAverageRDD2(rddList);
  }


  public Map<LocalDate, BigDecimal> attritionTimelineAvg(List<EventRecords> record) {

    return processor.attritionTimelineAvg(record);
  }



  public Map<LocalDate, BigDecimal> workforceTimelineAvg(List<EventRecords> record) {
    return processor.workforceTimelineAvg(record);

  }

  public Map<LocalDate, Long> workforceTimelineSingle(List<EventRecords> record) {
    return processor.workforceTimelineSingle(record);
  }


}
