package waqar_group.waqar_artifact;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Properties;

public class TwitterExample {

	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.setProperty(TwitterSource.CONSUMER_KEY, "DouhWR6wro8XbpLLEDqIF96cG");
		properties.setProperty(TwitterSource.CONSUMER_SECRET, "JbofqegWTTjIvzAeGxRvnTOGzpYzRiqO7IaVFyfbje7KzN56qF");
		properties.setProperty(TwitterSource.TOKEN, "1245167467-hPueDiv31Wm3R12EJ4pFhjow03paHw2Kl6Tr4rL");
		properties.setProperty(TwitterSource.TOKEN_SECRET, "KPzuWmq6JDWaSwdtpz4BmbDsRaqXjx058RmvxecIH4Pb6");
		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> streamSource;
		streamSource = env.addSource(new TwitterSource(properties));
		DataStream<Tuple4<String, String, String, Integer>> tweets = streamSource.filter(new FilterByFollowerCount(10)).filter(new FilterNonEnglishAndbyLocation("Pakistan")).flatMap(new SelectTweetsMap());
		System.out.println("Printing result to stdout. Use --output to specify output path.");
		// streamSource.print();
		tweets.print();
		// execute program
		env.execute("Twitter Streaming Example");
	}

	public static class FilterNonEnglishAndbyLocation implements FilterFunction<String> {
		String location = "";
		private static final long serialVersionUID = 1L;

		public FilterNonEnglishAndbyLocation(String location) {
			this.location = location;
		}

		@Override
		public boolean filter(String value) throws Exception {

			JsonNode jsonNode = parseValues(value);
			if (jsonNode != null) {
				boolean isEnglisherAndPakistani = jsonNode.has("user") && jsonNode.get("user").has("lang")
						&& jsonNode.get("user").has("location") && !jsonNode.get("user").get("location").isNull()
						&& jsonNode.get("user").get("lang").asText().equals("en")
						&& jsonNode.get("user").get("location").asText().contains(location);
//				if (jsonNode.has("user") && jsonNode.get("user").has("location")
//						&& !jsonNode.get("user").get("location").isNull()) {
//					System.out.println(jsonNode.get("user").get("location").asText());
//					System.out.println(isEnglishAndPakistani);
//				}

				boolean hasText = jsonNode.has("text");
				if (isEnglisherAndPakistani && hasText) {
					// String name = jsonNode.get("user").get("name").asText();
					// message of tweet

					String tweet = jsonNode.get("text").asText().toUpperCase();
					// System.out.println(tweet);
					if (!tweet.equals("")) {
						return true;
					}

				}
			}
			return false;
		}
	}

	public static class FilterByFollowerCount implements FilterFunction<String> {

		int count = 0;

		public FilterByFollowerCount(int count) {
			this.count = count;
		}

		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {

			JsonNode jsonNode = parseValues(value);
			if (jsonNode != null) {
				boolean hasFollowers = jsonNode.has("user") && jsonNode.get("user").has("followers_count")
						&& !jsonNode.get("user").get("followers_count").isNull();
				if (hasFollowers && jsonNode.get("user").get("followers_count").asInt() >= count)
					return true;
			}
			return false;
		}
	}

	public static class SelectTweetsMap implements FlatMapFunction<String, Tuple4<String, String, String, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public void flatMap(String value, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
			JsonNode jsonNode = parseValues(value);
			if (jsonNode != null) {
				String name = jsonNode.get("user").get("name").asText();
				String location = jsonNode.get("user").get("location").asText();
				// message of tweet
				String tweet = jsonNode.get("text").asText().toUpperCase();
				out.collect(new Tuple4<>(location, name, tweet, 1));
			}			
		}
	}

	private static JsonNode parseValues(String str) {
		ObjectMapper jsonParser = new ObjectMapper();
		try {
			return jsonParser.readValue(str, JsonNode.class);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private static final String IS_ENGLISH_REGEX = "^[ \\w \\d \\s \\. \\& \\+ \\- \\, \\! \\@ \\# \\$ \\% \\^ \\* \\( \\) \\; \\\\ \\/ \\| \\< \\> \\\" \\' \\? \\= \\: \\[ \\] ]*$";

	private static boolean isEnglish(String text) {
		if (text == null) {
			return false;
		}
		return text.matches(IS_ENGLISH_REGEX);
	}

}