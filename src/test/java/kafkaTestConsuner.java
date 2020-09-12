import com.huake.msg.kafka.utils.KafkaUtils;
import org.junit.Test;

public class kafkaTestConsuner {
	@Test
	public void testConsumer() {

		KafkaUtils.consumerClient("test");
	}

	@Test
	public void testForSubscribe() {

		KafkaUtils.recordForSubscribe("test").forEach(record -> {
			System.out.println("m2 offset : {" + record.offset() + "} , value : {" + record.value()
					+ "},channel :{ test } topic : {" + record.topic() + "}");
		});
	}

	@Test
	public void testForAssign() {

		KafkaUtils.recordForAssign("test").forEach(record -> {
			System.out.println("m2 offset : {" + record.offset() + "} , value : {" + record.value()
					+ "},channel :{ test } topic : {" + record.topic() + "}");
		});
	}
}
