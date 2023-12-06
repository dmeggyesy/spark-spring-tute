package li.zah.sparkspringpub;

import com.clearspring.analytics.util.Lists;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import li.zah.sparkspringpub.domain.EventRecord;
import li.zah.sparkspringpub.domain.EventRecords;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSpringPub implements Serializable {


  public static String test(JavaSparkContext context) {
    List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 15);
    JavaRDD<Integer> ds = context.parallelize(data);
    System.out.println("*** only one column, and it always has the same name");

    JavaRDD<Integer> res = ds.filter(x -> x > 12);

    System.out.println("*** values > 12");

    res.collect().forEach(System.out::println);

    System.out.println(res.collect());

    return res.collect().toString();
  }

  public static List<EventRecord> filterByEmpId(JavaSparkContext context, EventRecords record, String empId) {

    JavaRDD<List<String>> dataset = context.parallelize(record.getEvents());

    JavaRDD<EventRecord> eventRecordRDD = dataset.map(EventRecord::new);

    return eventRecordRDD.filter(r -> empId.equals(r.getEmployeeId())).collect();

  }


  public static JavaPairRDD<String, Iterable<EventRecord>> groupedByEmployeeId( JavaRDD<EventRecord> dataset) {
    return dataset.groupBy(EventRecord::getEmployeeId);
  }

  public static JavaRDD<EventRecord> loadEventRecord(JavaSparkContext context, EventRecords record) {

    JavaRDD<List<String>> dataset = context.parallelize(record.getEvents());

    return dataset.map(EventRecord::new);

  }

  public static Map<String, List<EventRecord>> getAllEventsByEmployees(JavaSparkContext context, EventRecords record) {

    JavaRDD<EventRecord> eventRecordRDD = loadEventRecord(context, record);

    return groupedByEmployeeId(eventRecordRDD).mapValues(v -> Lists.newArrayList(v))
        .collectAsMap();
  }









}
