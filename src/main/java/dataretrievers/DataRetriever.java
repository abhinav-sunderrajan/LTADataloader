package dataretrievers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author abhinav.sunderrajan
 * 
 */
public class DataRetriever<T> implements Runnable {

	private Queue<T> dataQueue;
	private String accountKey;
	private String userId;
	String apiUrl;
	private static final Logger LOGGER = Logger.getLogger(DataRetriever.class);
	private static final JSONParser JSON_PARSER = new JSONParser();

	/**
	 * Retrieves data from the LTA API URL.
	 * 
	 * @param apiUrl
	 * @param configproperties
	 * @param xmlDocQueueMap
	 */
	public DataRetriever(String apiUrl, Properties configproperties, Queue<T> xmlDocQueueMap)
			throws IOException {
		this.dataQueue = xmlDocQueueMap;
		this.apiUrl = apiUrl;
		this.accountKey = configproperties.getProperty("AccountKey");
		this.userId = configproperties.getProperty("UniqueUserID");
	}

	@Override
	public void run() {
		URL url = null;
		try {
			url = new URL(apiUrl);
			URLConnection conn = url.openConnection();
			conn.setConnectTimeout(2000);
			conn.setReadTimeout(5000);
			conn.setRequestProperty("accept", "*/*");
			conn.addRequestProperty("AccountKey", accountKey);
			conn.addRequestProperty("UniqueUserID", userId);

			StringBuilder strBuilder = new StringBuilder();
			InputStreamReader is = new InputStreamReader(conn.getInputStream());
			BufferedReader br = new BufferedReader(is);
			boolean goodDocument = true;
			if (br.ready()) {
				long t = System.currentTimeMillis();
				while (true) {
					String line = br.readLine();
					if (line == null || line == "") {
						break;
					} else {
						strBuilder.append(line);
					}

					if (System.currentTimeMillis() - t > 5000) {
						LOGGER.info("Breaking due to a weird web-service error " + line);
						goodDocument = false;
						break;
					}
				}
				if (strBuilder.toString().startsWith("<") && goodDocument) {
					Document document = DocumentHelper.parseText(strBuilder.toString());
					if (document.getRootElement().element("entry") != null) {
						LOGGER.info("received document from web-service: " + url.getFile());
						dataQueue.add((T) document);
					} else {
						LOGGER.error("recived a bad document from the web-service");
					}
				}

				if (isJSONValid(strBuilder.toString())) {
					JSONObject obj = (JSONObject) JSON_PARSER.parse(strBuilder.toString());
					JSONArray travelTimes = (JSONArray) obj.get("value");
					System.out.println(travelTimes.get(1));
				}

				is.close();
			}

		} catch (DocumentException e) {
			LOGGER.error("Error parsing XML from " + url.getHost(), e);
		} catch (IOException e) {
			LOGGER.error("Error reading the XML document from " + url.getFile(), e);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean isJSONValid(String test) {
		try {
			JSON_PARSER.parse(test);
		} catch (ParseException e) {
			return false;
		}
		return true;
	}
}
