package dataretrievers;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import utils.DatabaseAccess;

public class IncidentDatabaseLoader extends DatabaseLoader {

	private long clear = 0;
	private Map<Long, Long> insertedMap;
	private static final Logger LOGGER = Logger.getLogger(IncidentDatabaseLoader.class);
	private static final DateTimeFormatter df = DateTimeFormat
			.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

	/**
	 * 
	 * @param xmlDocQueue
	 * @param access
	 */
	public IncidentDatabaseLoader(Queue<Document> xmlDocQueue, DatabaseAccess access) {
		super(xmlDocQueue, access);
		insertedMap = new HashMap<Long, Long>();
	}

	@Override
	public void run() {
		while (true) {
			while (xmlDocQueue.isEmpty()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			Document data = xmlDocQueue.poll();
			clear++;

			@SuppressWarnings("unchecked")
			Iterator<Element> it = data.getRootElement().elementIterator("entry");
			int nRows = 0;
			try {
				while (it.hasNext()) {
					Element element = it.next();
					Element content = element.element("content").element("properties");

					Long id = Long.parseLong(content.elementText("IncidentID"));
					String message = content.elementText("Message").replaceAll("[^\\w\\s]", "");
					Double latitude = Double.parseDouble(content.elementText("Latitude"));
					Double longitude = Double.parseDouble(content.elementText("Longitude"));
					String type = content.elementText("Type").replaceAll("[^\\w\\s]", "");
					Double distance = Double.parseDouble(content.elementText("Distance"));
					Long time_stamp = df.parseDateTime(content.elementText("CreateDate"))
							.getMillis();

					if (!insertedMap.containsKey(id)) {
						insertedMap.put(id, time_stamp);
						access.executeBlockUpdate(id, message, latitude, longitude, distance, type,
								time_stamp);
						nRows++;
					}
				}
			} catch (SQLException e) {
				LOGGER.error("Error inserting data to database.", e);
			}
			if (clear % 100 == 0) {
				insertedMap.clear();
			}
			LOGGER.info("Inserted " + nRows + " incident data rows corresponding to batch: "
					+ clear + ", shall poll LTA web-service after two minutes");
			nRows = 0;

		}

	}
}
