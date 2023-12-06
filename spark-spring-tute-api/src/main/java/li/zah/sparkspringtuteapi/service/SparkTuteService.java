package li.zah.sparkspringtuteapi.service;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import li.zah.sparkspringtuteapi.domain.AverageCombiner;
import li.zah.sparkspringtuteapi.domain.EventRecord;
import li.zah.sparkspringtuteapi.domain.EventRecords;
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

  public String test() {

    List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 15);
    JavaRDD<Integer> ds = sparkContext.parallelize(data);
    System.out.println("*** only one column, and it always has the same name");

    JavaRDD<Integer> res = ds.filter(x -> x > 12);

    System.out.println("*** values > 12");

    res.collect().forEach(System.out::println);

    System.out.println(res);

    return res.toString();
  }

  public List<EventRecord> filterByEmpId(EventRecords record, String empId) {

    JavaRDD<List<String>> dataset = sparkContext.parallelize(record.getEvents());

    JavaRDD<EventRecord> eventRecordRDD = dataset.map(EventRecord::new);

    return eventRecordRDD.filter(r -> empId.equals(r.getEmployeeId())).collect();

  }

  public JavaPairRDD<String, Iterable<EventRecord>> groupedByEmployeeId(JavaRDD<EventRecord> dataset) {
    return dataset.groupBy(EventRecord::getEmployeeId);
  }

  public JavaRDD<EventRecord> loadEventRecord(EventRecords record) {

    JavaRDD<List<String>> dataset = sparkContext.parallelize(record.getEvents());

    return dataset.map(EventRecord::new);

  }

  public Map<String, List<EventRecord>> getAllEventsByEmployees(EventRecords record) {

    JavaRDD<EventRecord> eventRecordRDD = loadEventRecord(record);

    return groupedByEmployeeId(eventRecordRDD).mapValues(v -> Lists.newArrayList(v))
        .collectAsMap();
  }

  public Long getNumberOfEmployeesAtTimestep(EventRecords record, LocalDate date) {

    LocalDate simStart = record.getSimulationStart();

    JavaRDD<EventRecord> eventRecordRDD = loadEventRecord(record);

    JavaPairRDD<String, Iterable<EventRecord>> groupedByEmpIdRDD = groupedByEmployeeId(eventRecordRDD);

    //  System.out.println(groupedByEmpIdRDD.count());

    long numberOfEmployeesAtTimestep = groupedByEmpIdRDD.filter(pair -> {
      Iterator<EventRecord> events = pair._2().iterator();

      //System.out.println("emp : "+ pair._1());
      EventRecord lastEvent = null;
      while (events.hasNext()) {
        EventRecord ev = events.next();
        long timestamp = Long.parseLong(ev.getTimestamp());
        LocalDate evDate = simStart.plusDays(timestamp);

        if (evDate.isAfter(date)) {
          break;
        }
        lastEvent = ev;
      }
      if (lastEvent == null) {
        return false;
      }
      if (lastEvent.getEventType().equals("ATTRITION")) {
        return false;
      }
      return true;

    }).count();

    // Map<String, List<EventRecord>> groupedByEmpIdMap = groupedByEmpIdRDD.mapValues(v -> Lists.newArrayList(v)).collectAsMap();

    return numberOfEmployeesAtTimestep;
  }

  @Deprecated
  public Long getNumberOfEmployeesAtTimestepOld(EventRecords record, LocalDate date) {

    LocalDate simStart = record.getSimulationStart();
    System.out.println("sim start: " + simStart);
    JavaRDD<List<String>> dataset = sparkContext.parallelize(record.getEvents());

    JavaRDD<EventRecord> eventRecordRDD = dataset.map(EventRecord::new);

    JavaPairRDD<String, Iterable<EventRecord>> groupedByEmpIdRDD = eventRecordRDD.groupBy(
        eventRecord -> eventRecord.getEmployeeId());

    Map<String, List<EventRecord>> groupedByEmpIdMap = groupedByEmpIdRDD.mapValues(v -> Lists.newArrayList(v))
        .collectAsMap();

    long numberOfEmployeesAtTimestep = groupedByEmpIdMap.entrySet().parallelStream().filter(pair -> {
      Iterator<EventRecord> events = pair.getValue().iterator();

      EventRecord lastEvent = null;
      while (events.hasNext()) {
        EventRecord ev = events.next();
        long timestamp = Long.parseLong(ev.getTimestamp());
        LocalDate evDate = simStart.plusDays(timestamp);

        if (evDate.isAfter(date)) {
          break;
        }
        lastEvent = ev;
      }
      if (lastEvent == null) {
        return false;
      }
      if (lastEvent.getEventType().equals("ATTRITION")) {
        return false;
      }
      return true;

    }).count();

    return numberOfEmployeesAtTimestep;
  }

  public Long attritionInRange(EventRecords record, LocalDate rangeStart, LocalDate rangeEnd) {

    LocalDate simStart = record.getSimulationStart();

    JavaRDD<List<String>> dataset = sparkContext.parallelize(record.getEvents());

    JavaRDD<EventRecord> eventRecordRDD = dataset.map(EventRecord::new);

    JavaPairRDD<String, Iterable<EventRecord>> groupedByEmpIdRDD = eventRecordRDD.groupBy(
        eventRecord -> eventRecord.getEmployeeId());

    //  System.out.println(groupedByEmpIdRDD.count());

    long numberOfEmployeesAtTimestep = groupedByEmpIdRDD.filter(pair -> {
      Iterator<EventRecord> events = pair._2().iterator();

      //System.out.println("emp : "+ pair._1());
      EventRecord lastEvent = null;
      while (events.hasNext()) {
        EventRecord ev = events.next();
        long timestamp = Long.parseLong(ev.getTimestamp());
        LocalDate evDate = simStart.plusDays(timestamp);
        if (evDate.isAfter(rangeEnd)) {
          break;
        }
        lastEvent = ev;
      }
      if (lastEvent == null) {
        return false;
      }
      LocalDate evDate = simStart.plusDays(Long.parseLong(lastEvent.getTimestamp()));
      if (lastEvent.getEventType().equals("ATTRITION") && evDate.isAfter(rangeStart)) {
        return true;
      }
      return false;

    }).count();

    return numberOfEmployeesAtTimestep;
  }

  public Map<String, LocalDate> attritionDays(EventRecords records) {
    LocalDate simStart = records.getSimulationStart();

    List<LocalDate> datesToEvaluate = new ArrayList<>();

    for (int i = 0; i < 10 * 12; i++) {
      datesToEvaluate.add(simStart.plusMonths(i).withDayOfMonth(1));
    }

    JavaRDD<List<String>> dataset = sparkContext.parallelize(records.getEvents());

    JavaRDD<EventRecord> eventRecordRDD = dataset.map(EventRecord::new);

    JavaPairRDD<String, Iterable<EventRecord>> groupedByEmpIdRDD = eventRecordRDD.groupBy(
        eventRecord -> eventRecord.getEmployeeId());

    JavaPairRDD<String, LocalDate> attritionCountsRDD = groupedByEmpIdRDD.mapValues(v -> {
      for (EventRecord ev : v) {
        if (ev.getEventType().equals("ATTRITION") && StringUtils.isNumeric(ev.getTimestamp())) {
          return simStart.plusDays(Long.parseLong(ev.getTimestamp()));
        }
      }
      return null;
    });

    return attritionCountsRDD.collectAsMap();
  }

  @Deprecated
  public Map<LocalDate, Long> attritionTimelineOld(EventRecords records) {
    LocalDate simStart = records.getSimulationStart();

    List<LocalDate> datesToEvaluate = new ArrayList<>();

    for (int i = 0; i < 10 * 12; i++) {
      datesToEvaluate.add(simStart.plusMonths(i).withDayOfMonth(1));
    }

    JavaRDD<List<String>> dataset = sparkContext.parallelize(records.getEvents());

    JavaRDD<EventRecord> eventRecordRDD = dataset.map(EventRecord::new);

    JavaPairRDD<String, Iterable<EventRecord>> groupedByEmpIdRDD = eventRecordRDD.groupBy(
        eventRecord -> eventRecord.getEmployeeId());

    JavaPairRDD<String, LocalDate> attritionCountsRDD = groupedByEmpIdRDD.mapValues(v -> {
      for (EventRecord ev : v) {
        if (ev.getEventType().equals("ATTRITION") && StringUtils.isNumeric(ev.getTimestamp())) {
          return simStart.plusDays(Long.parseLong(ev.getTimestamp()));
        }
      }
      return null;
    });

    JavaPairRDD<LocalDate, Long> monthlyAttritionRDD = attritionCountsRDD.filter(val -> val._2() != null)
        .mapToPair(pair -> {
          // Extract employee ID and quit date
          String empId = pair._1();
          LocalDate quitDate = pair._2();

          // Create LocalDate representing "month/year"
          LocalDate monthYear = LocalDate.of(quitDate.getYear(), quitDate.getMonth(), 1);

          // Return key-value pair with "month/year" and count of 1
          return new Tuple2<>(monthYear, 1L);
        });

    JavaPairRDD<LocalDate, Long> monthlyAttrCounts = monthlyAttritionRDD.reduceByKey((a, b) -> a + b);

    return new TreeMap<>(monthlyAttrCounts.collectAsMap());
  }

  public JavaPairRDD<LocalDate, Long> attritionTimeline2(EventRecords records) {
    LocalDate simStart = records.getSimulationStart();

    JavaRDD<EventRecord> eventRecordRDD = loadEventRecord(records);

    JavaPairRDD<String, Iterable<EventRecord>> groupedByEmpIdRDD = groupedByEmployeeId(eventRecordRDD);

    JavaPairRDD<String, LocalDate> attritionCountsRDD = groupedByEmpIdRDD.mapValues(v -> {
      for (EventRecord ev : v) {
        if (ev.getEventType().equals("ATTRITION") && StringUtils.isNumeric(ev.getTimestamp())) {
          return simStart.plusDays(Long.parseLong(ev.getTimestamp()));
        }
      }
      return null;
    });

    JavaPairRDD<LocalDate, Long> monthlyAttritionRDD = attritionCountsRDD.filter(val -> val._2() != null)
        .mapToPair(pair -> {
          // Extract employee ID and quit date
          String empId = pair._1();
          LocalDate quitDate = pair._2();

          // Create LocalDate representing "month/year"
          LocalDate monthYear = LocalDate.of(quitDate.getYear(), quitDate.getMonth(), 1);

          // Return key-value pair with "month/year" and count of 1
          return new Tuple2<>(monthYear, 1L);
        });

    JavaPairRDD<LocalDate, Long> monthlyAttrCounts = monthlyAttritionRDD.reduceByKey((a, b) -> a + b);

    System.out.println(monthlyAttrCounts.collectAsMap().keySet());

    return monthlyAttrCounts;
  }

  @Deprecated
  public Map<LocalDate, BigDecimal> oldAverageRDDs(List<JavaPairRDD<LocalDate, Long>> datasets) {
    // union datasets.
    System.out.println("datasets: " + datasets.size());
    Set<LocalDate> keySet = new HashSet<>();

    for (JavaPairRDD<LocalDate, Long> data : datasets) {
      keySet.addAll(data.keys().collect());
    }

    Map<LocalDate, Iterable<Long>> zeroMap = new HashMap<>();
    keySet.stream().forEach(k -> zeroMap.put(k, new ArrayList<>()));

    JavaPairRDD<LocalDate, Iterable<Long>> combined = null;
    for (JavaPairRDD<LocalDate, Long> data : datasets) {
      if (combined == null) {
        combined = data.mapValues(List::of);
      } else {

        combined.cogroup(data);
      }
    }
    if (combined == null) {
      throw new NullPointerException();
    }

    System.out.println("combi");
    System.out.println(combined.collectAsMap().keySet());

    BigDecimal denominator = new BigDecimal(datasets.size());

    JavaPairRDD<LocalDate, BigDecimal> total = combined.mapValues(values -> {
      long sum = 0;
      for (Long val : values) {
        sum += val;
      }
      return new BigDecimal(sum).divide(denominator, 2, RoundingMode.HALF_UP);

    });

    return new TreeMap<>(total.collectAsMap());

  }

  @Deprecated
  public Map<LocalDate, BigDecimal> oldAverageRDD2(List<JavaPairRDD<LocalDate, Long>> rddList) {
    JavaPairRDD<LocalDate, AverageCombiner> combined = null;
    JavaPairRDD<LocalDate, Long> zeroRdd = null;

    for (JavaPairRDD<LocalDate, Long> rdd : rddList) {
      if (zeroRdd == null) {
        zeroRdd = rdd.mapValues(value -> 0L);
      } else {
        zeroRdd = zeroRdd.union(rdd.mapValues(value -> 0L));
      }
    }

    zeroRdd = zeroRdd.reduceByKey((a, b) -> 0L);

    for (JavaPairRDD<LocalDate, Long> rdd : rddList) {
      JavaPairRDD<LocalDate, Long> rddWithZeros = zeroRdd.union(rdd);
      if (combined == null) {
        combined = rddWithZeros.mapValues(value -> {
          AverageCombiner combiner = new AverageCombiner();
          combiner.add(value);
          return combiner;
        });
      } else {
        combined = combined.union(rddWithZeros.mapValues(value -> {
          AverageCombiner combiner = new AverageCombiner();
          combiner.add(value);
          return combiner;
        })).reduceByKey((a, b) -> {
          a.add(b.getSum().longValue());
          return a;
        });
      }
    }

    JavaPairRDD<LocalDate, BigDecimal> result = combined.mapValues(AverageCombiner::average);

    return new TreeMap<>(result.collectAsMap());
  }

  public Map<LocalDate, BigDecimal> averageRDD3(List<JavaPairRDD<LocalDate, Long>> dataSet) {
    JavaPairRDD<LocalDate, Long> combined = null;

    for (JavaPairRDD<LocalDate, Long> rdd : dataSet) {
      if (combined == null) {
        combined = rdd;
      } else {
        combined = combined.union(rdd).reduceByKey(Long::sum);
      }
    }

    JavaPairRDD<LocalDate, BigDecimal> result = combined.mapValues(
        sum -> BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(dataSet.size()), 2, RoundingMode.HALF_UP));

    return result.collectAsMap();
  }

  public Map<LocalDate, BigDecimal> attritionTimelineAvg(List<EventRecords> record) {
    List<JavaPairRDD<LocalDate, Long>> res = new ArrayList<>();

    for (EventRecords rec : record) {
      res.add(attritionTimeline2(rec));
    }

    return new TreeMap<>(averageRDD3(res));

  }

  public Map<String, Long> getWorkforceTimeline(EventRecords record) {
    LocalDate simStart = record.getSimulationStart();

    int simEnd = Days.between(simStart, simStart.plusYears(10)).getAmount();

    List<LocalDate> dates = new ArrayList<>();

    for (int i = 0; i < 10 * 12; i++) {
      dates.add(simStart.plusMonths(i).withDayOfMonth(1));
    }

    JavaRDD<EventRecord> eventRecordRDD = loadEventRecord(record);

    JavaPairRDD<String, Iterable<EventRecord>> employeeEvents = groupedByEmployeeId(eventRecordRDD);

    JavaPairRDD<String, Tuple2<Long, Long>> employeeStartEnds = employeeEvents.mapValues(value -> {
      Long activeTimestamp = null;
      Long endTimestamp = null;
      EventRecord finalEvent = null;

      Iterator<EventRecord> iterator = value.iterator();
      while (iterator.hasNext()) {
        EventRecord rec = iterator.next();
        if (activeTimestamp == null) {
          activeTimestamp = Long.parseLong(rec.getTimestamp());
        }
        finalEvent = rec;
      }
      if (finalEvent.getEventType().equals("ATTRITION")) {
        endTimestamp = Long.parseLong(finalEvent.getTimestamp());
      } else {
        endTimestamp = (long) (simEnd);
      }

      return new Tuple2<Long, Long>(activeTimestamp, endTimestamp);
    });

    System.out.println(employeeStartEnds.collectAsMap());

    return null;

  }

  public JavaPairRDD<LocalDate, Long> getWorkforceTimeline2(EventRecords record) {
    LocalDate simStart = record.getSimulationStart();

    int simEnd = Days.between(simStart, simStart.plusYears(10)).getAmount();

    List<LocalDate> dateList = new ArrayList<>();

    for (int i = 0; i < 10 * 12; i++) {
      dateList.add(simStart.plusMonths(i).withDayOfMonth(1));
    }

    JavaRDD<EventRecord> eventRecordRDD = loadEventRecord(record);

    JavaPairRDD<String, Iterable<EventRecord>> employeeEvents = groupedByEmployeeId(eventRecordRDD);

    JavaPairRDD<String, Tuple2<LocalDate, LocalDate>> employeeStartEnds = employeeEvents.mapValues(value -> {
      Long activeTimestamp = null;
      Long endTimestamp = null;
      EventRecord finalEvent = null;

      Iterator<EventRecord> iterator = value.iterator();
      while (iterator.hasNext()) {
        EventRecord rec = iterator.next();
        if (activeTimestamp == null) {
          activeTimestamp = Long.parseLong(rec.getTimestamp());
        }
        finalEvent = rec;
      }
      if (finalEvent.getEventType().equals("ATTRITION")) {
        endTimestamp = Long.parseLong(finalEvent.getTimestamp());
      } else {
        endTimestamp = (long) (simEnd);
      }

      return new Tuple2<>(simStart.plusDays(activeTimestamp), simStart.plusDays(endTimestamp));
    });

    JavaPairRDD<LocalDate, Tuple2<String, Tuple2<LocalDate, LocalDate>>> flattened = employeeStartEnds.flatMapToPair(
        employee -> {
          String id = employee._1();
          Tuple2<LocalDate, LocalDate> dates = employee._2();
          ArrayList<Tuple2<LocalDate, Tuple2<String, Tuple2<LocalDate, LocalDate>>>> results = new ArrayList<>();
          for (LocalDate timestamp : dateList) {
            results.add(new Tuple2<>(timestamp, new Tuple2<>(id, dates)));
          }
          return results.iterator();
        });

    // Filter active employees
    JavaPairRDD<LocalDate, Tuple2<String, Tuple2<LocalDate, LocalDate>>> active = flattened.filter(rec -> {
      LocalDate timestamp = rec._1();
      Tuple2<String, Tuple2<LocalDate, LocalDate>> employee = rec._2();
      LocalDate startDate = employee._2()._1();
      LocalDate endDate = employee._2()._2();
      return (startDate.isBefore(timestamp) || startDate.isEqual(timestamp)) && (endDate.isAfter(timestamp)
          || endDate.isEqual(timestamp));
    });

    // Count active employees for each timestamp
    return active.mapValues(v -> 1L).aggregateByKey(0L, Long::sum, Long::sum);

    //    counts.collect().forEach(System.out::println);

    //  return new TreeMap<>(counts.collectAsMap()) ;

  }

  public Map<LocalDate, BigDecimal> workforceTimelineAvg(List<EventRecords> record) {
    List<JavaPairRDD<LocalDate, Long>> res = new ArrayList<>();

    for (EventRecords rec : record) {
      res.add(getWorkforceTimeline2(rec));
    }

    return new TreeMap<>(averageRDD3(res));

  }

  public Map<LocalDate, Long> workforceTimelineSingle(List<EventRecords> record) {
    return new TreeMap<>(getWorkforceTimeline2(record.get(0)).collectAsMap());
  }

  public JavaPairRDD<LocalDate, Long> getWorkforceTimeline3(EventRecords record) {
    LocalDate simStart = record.getSimulationStart();

    int simEnd = Days.between(simStart, simStart.plusYears(10)).getAmount();

    List<LocalDate> dateList = new ArrayList<>();

    for (int i = 0; i < 10 * 12; i++) {
      dateList.add(simStart.plusMonths(i).withDayOfMonth(1));
    }

    JavaRDD<LocalDate> dateRDD = sparkContext.parallelize(dateList);

    JavaRDD<EventRecord> eventRecordRDD = loadEventRecord(record);

    JavaPairRDD<String, Iterable<EventRecord>> employeeEvents = groupedByEmployeeId(eventRecordRDD);

    JavaPairRDD<String, Tuple2<LocalDate, LocalDate>> employeeStartEnds = employeeEvents.mapValues(value -> {
      Long activeTimestamp = null;
      Long endTimestamp = null;
      EventRecord finalEvent = null;

      Iterator<EventRecord> iterator = value.iterator();
      while (iterator.hasNext()) {
        EventRecord rec = iterator.next();
        if (activeTimestamp == null) {
          activeTimestamp = Long.parseLong(rec.getTimestamp());
        }
        finalEvent = rec;
      }
      if (finalEvent.getEventType().equals("ATTRITION")) {
        endTimestamp = Long.parseLong(finalEvent.getTimestamp());
      } else {
        endTimestamp = (long) (simEnd);
      }

      return new Tuple2<>(simStart.plusDays(activeTimestamp), simStart.plusDays(endTimestamp));
    });

    JavaPairRDD<Tuple2<String, Tuple2<LocalDate, LocalDate>>, LocalDate> cartesian = employeeStartEnds.cartesian(
        dateRDD);

    JavaPairRDD<LocalDate, Tuple2<String, Tuple2<LocalDate, LocalDate>>> flattened = cartesian.mapToPair(rec -> {
      String id = rec._1()._1();
      Tuple2<LocalDate, LocalDate> dates = rec._1()._2();
      LocalDate timestamp = rec._2();
      return new Tuple2<>(timestamp, new Tuple2<>(id, dates));
    });

    // Filter active employees
    JavaPairRDD<LocalDate, Tuple2<String, Tuple2<LocalDate, LocalDate>>> active = flattened.filter(rec -> {
      LocalDate timestamp = rec._1();
      Tuple2<String, Tuple2<LocalDate, LocalDate>> employee = rec._2();
      LocalDate startDate = employee._2()._1();
      LocalDate endDate = employee._2()._2();
      return (startDate.isBefore(timestamp) || startDate.isEqual(timestamp)) && (endDate.isAfter(timestamp)
          || endDate.isEqual(timestamp));
    });

    // Count active employees for each timestamp
    return active.mapValues(v -> 1L).aggregateByKey(0L, Long::sum, Long::sum);

  }

}
