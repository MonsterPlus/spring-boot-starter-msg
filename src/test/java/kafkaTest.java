import cn.hutool.json.JSONUtil;
import com.huake.msg.kafka.AsyncMsgManager;
import com.huake.msg.kafka.Message;
import com.huake.msg.kafka.mode.RetryCallback;
import com.huake.msg.kafka.mode.impl.BackOffAlgorithmRetryModeImpl;
import com.huake.msg.kafka.utils.KafkaUtils;
import com.huake.msg.kafka.utils.RetryUtil;
import com.huake.msg.kafka.utils.SpringFactoriesUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class kafkaTest {
	@Test
	public void test() {
		try {
			List<String> msg = new ArrayList<>();
			msg.add("你好！");
			msg.add("kafka");
			AsyncMsgManager.getInstance().pushMsgToKafka(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMessageString() {
		try {
			List<Message> msgList = new ArrayList<>();
			Message msg = new Message();
			Map<String, Object> headerMap = new HashMap<>();
			Map<String, Object> bodyMap = new HashMap<>();
			Map<String, Object> oj = new HashMap<>();
			Map<String, Object> resume = new HashMap<>();
			oj.put("name", "章北海");
			oj.put("age", 43);
			oj.put("gender", 1);
			headerMap.put("user", oj);
			resume.put("School", "银河军事学院");
			resume.put("Record1", "中华人民共和国 亚洲舰队 蓝色空间星际战列舰舰长 ");
			resume.put("Record2", "为地球文明保留最后的火种");
			bodyMap.put("resume", resume);
			msg.setHeaders(headerMap);
			msg.setBody(bodyMap);

			msgList.add(msg);

			Message msg2 = new Message();
			Map<String, Object> headerMap2 = new HashMap<>();
			Map<String, Object> oj2 = new HashMap<>();
			Map<String, Object> resume2 = new HashMap<>();
			Map<String, Object> bodyMap2 = new HashMap<>();
			oj2.put("name", "罗辑");
			oj2.put("age", 24);
			oj2.put("gender", 1);
			headerMap2.put("user", oj2);
			resume2.put("School", "清华大学");
			resume2.put("Record1", "清华大学社会关系学教授 ");
			resume2.put("Record2", "宇宙社会学创始人");
			resume2.put("Record3", "人类面壁计划亚洲区面壁者");
			resume2.put("Record4", "威慑纪元执剑人");
			resume2.put("Record5", "守护人类文明，威慑三体文明侵略之先行者");
			bodyMap2.put("resume", resume2);
			msg2.setHeaders(headerMap2);
			msg2.setBody(bodyMap2);
			msgList.add(msg2);
			String jsonString = JSONUtil.parse(msgList).toJSONString(3);
			List<String> strList = new ArrayList<String>();
			strList.add(jsonString);
			AsyncMsgManager.getInstance().pushMsgToKafka(strList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testMessage() {
		try {
			List<Message> msgList = new ArrayList<>();
			Message msg = new Message();
			Map<String, Object> headerMap = new HashMap<>();
			Map<String, Object> bodyMap = new HashMap<>();
			Map<String, Object> oj = new HashMap<>();
			Map<String, Object> resume = new HashMap<>();
			oj.put("name", "章北海");
			oj.put("age", 43);
			oj.put("gender", 1);
			headerMap.put("user", oj);
			resume.put("School", "银河军事学院");
			resume.put("Record1", "中华人民共和国 亚洲舰队 蓝色空间星际战列舰舰长 ");
			resume.put("Record2", "为地球文明保留最后的火种");
			bodyMap.put("resume", resume);
			msg.setHeaders(headerMap);
			msg.setBody(bodyMap);

			msgList.add(msg);

			Message msg2 = new Message();
			Map<String, Object> headerMap2 = new HashMap<>();
			Map<String, Object> oj2 = new HashMap<>();
			Map<String, Object> resume2 = new HashMap<>();
			Map<String, Object> bodyMap2 = new HashMap<>();
			oj2.put("name", "罗辑");
			oj2.put("age", 24);
			oj2.put("gender", 1);
			headerMap2.put("user", oj2);
			resume2.put("School", "清华大学");
			resume2.put("Record1", "清华大学社会关系学教授 ");
			resume2.put("Record2", "宇宙社会学创始人");
			resume2.put("Record3", "人类面壁计划亚洲区面壁者");
			resume2.put("Record4", "威慑纪元执剑人");
			resume2.put("Record5", "守护人类文明，威慑三体文明侵略之先行者");
			bodyMap2.put("resume", resume2);
			msg2.setHeaders(headerMap2);
			msg2.setBody(bodyMap2);
			msgList.add(msg2);
			AsyncMsgManager.getInstance().pushMsgToKafka(msgList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testRetry() {
		RetryCallback callback = SpringFactoriesUtils.loadFactories(RetryCallback.class);
		try {
			List<Message> msgList = new ArrayList<>();
			Message msg = new Message();
			Map<String, Object> headerMap = new HashMap<>();
			Map<String, Object> bodyMap = new HashMap<>();
			Map<String, Object> oj = new HashMap<>();
			Map<String, Object> resume = new HashMap<>();
			oj.put("name", "章北海");
			oj.put("age", 43);
			oj.put("gender", 1);
			headerMap.put("user", oj);
			resume.put("School", "银河军事学院");
			resume.put("Record1", "中华人民共和国 亚洲舰队 蓝色空间星际战列舰舰长 ");
			resume.put("Record2", "为地球文明保留最后的火种");
			bodyMap.put("resume", resume);
			msg.setHeaders(headerMap);
			msg.setBody(bodyMap);

			msgList.add(msg);

			Message msg2 = new Message();
			Map<String, Object> headerMap2 = new HashMap<>();
			Map<String, Object> oj2 = new HashMap<>();
			Map<String, Object> resume2 = new HashMap<>();
			Map<String, Object> bodyMap2 = new HashMap<>();
			oj2.put("name", "罗辑");
			oj2.put("age", 24);
			oj2.put("gender", 1);
			headerMap2.put("user", oj2);
			resume2.put("School", "清华大学");
			resume2.put("Record1", "清华大学社会关系学教授 ");
			resume2.put("Record2", "宇宙社会学创始人");
			resume2.put("Record3", "人类面壁计划亚洲区面壁者");
			resume2.put("Record4", "威慑纪元执剑人");
			resume2.put("Record5", "守护人类文明，威慑三体文明侵略之先行者");
			bodyMap2.put("resume", resume2);
			msg2.setHeaders(headerMap2);
			msg2.setBody(bodyMap2);
			msgList.add(msg2);
			String jsonString = JSONUtil.parse(msgList).toJSONString(3).replace("\n", "").replace("\n", "");
			ProducerRecord record = new ProducerRecord(KafkaUtils.channelSelecter(null).getProducerChannel().getTopic(),
					jsonString);
			RetryUtil.resend(2, new BackOffAlgorithmRetryModeImpl(), record, callback);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
