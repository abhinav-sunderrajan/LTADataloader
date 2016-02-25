package dataretrievers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

public class DataRetriever implements Runnable {

	private Queue<Document> xmlDocQueue;
	private URL url;
	private String accountKey;
	private String userId;
	private static final Logger LOGGER = Logger.getLogger(DataRetriever.class);

	// account-key 59bHkENlIppXHH+OWE9jhg==
	// unique-user-id 08b95813-3064-490d-8151-542cbe457e70

	public DataRetriever(String apiUrl, Properties configproperties,
			Map<String, Queue<Document>> xmlDocQueueMap) throws IOException {
		this.xmlDocQueue = xmlDocQueueMap.get(apiUrl);
		this.url = new URL(apiUrl);
		this.accountKey = configproperties.getProperty("AccountKey");
		this.userId = configproperties.getProperty("UniqueUserID");
	}

	@Override
	public void run() {
		try {

			URLConnection conn = url.openConnection();
			conn.setRequestProperty("accept", "*/*");
			conn.addRequestProperty("AccountKey", accountKey);
			conn.addRequestProperty("UniqueUserID", userId);

			StringBuilder strBuilder = new StringBuilder();
			InputStreamReader is = new InputStreamReader(conn.getInputStream());
			BufferedReader br = new BufferedReader(is);
			while (true) {
				String line = br.readLine();
				if (line == null) {
					break;
				} else {
					strBuilder.append(line);
				}
			}
			Document document = DocumentHelper.parseText(strBuilder.toString());

			if (document.getRootElement().element("entry").elements().size() > 0) {
				xmlDocQueue.add(document);
			} else {
				System.out.println(document.asXML());
			}

			is.close();
		} catch (DocumentException e) {
			LOGGER.error("Error parsing XML from " + url.getHost(), e);
		} catch (IOException e) {
			LOGGER.error("Error reading the XML document from " + url.getHost(), e);
		}

	}
}
