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

/**
 * Pushes the traffic speed band real time data into a postGres database.
 * 
 * @author abhinav.sunderrajan
 * 
 */
public class TrafficSpeedBandDatabaseLoader extends DatabaseLoader {

	private long clear = 0;
	private Map<String, Long> insertedMap;
	private static final Logger LOGGER = Logger.getLogger(TrafficSpeedBandDatabaseLoader.class);
	private static final DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

	/**
	 * 
	 * @param xmlDocQueue
	 * @param access
	 */
	public TrafficSpeedBandDatabaseLoader(Queue<Document> xmlDocQueue, DatabaseAccess access) {
		super(xmlDocQueue, access);
		insertedMap = new HashMap<String, Long>();
	}

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
					String id = content.elementText("TrafficSpeedBandID");
					Long linkId = Long.parseLong(content.elementText("LinkID"));
					String roadname = content.elementText("RoadName").replaceAll("[^\\w\\s]", "");
					char roadCategory = content.elementText("RoadCategory").charAt(0);
					Integer band = Integer.parseInt(content.elementText("Band"));
					Double minSpeed = 40.0;
					if (!(content.elementText("MinimumSpeed") == null || content
							.elementText("MinimumSpeed") == "")) {
						minSpeed = Double.parseDouble(content.elementText("MinimumSpeed"));
					}
					Double maxSpeed = 59.0;
					if (!(content.elementText("MaximumSpeed") == null || content
							.elementText("MaximumSpeed") == "")) {
						maxSpeed = Double.parseDouble(content.elementText("MaximumSpeed"));
					}

					String[] latLons = content.elementText("Location").split(" ");
					Double lat1 = Double.parseDouble(latLons[0]);
					Double lon1 = Double.parseDouble(latLons[1]);
					Double lat2 = Double.parseDouble(latLons[2]);
					Double lon2 = Double.parseDouble(latLons[3]);
					String summary = content.elementText("Summary").replaceAll("[^\\w\\s]", "");
					Long time_stamp = df.parseDateTime(content.elementText("CreateDate"))
							.getMillis();

					if (!insertedMap.containsKey(id)) {
						insertedMap.put(id, time_stamp);
						access.executeBlockUpdate(id, linkId, roadname, roadCategory, band,
								minSpeed, maxSpeed, lat1, lon1, lat2, lon2, summary, time_stamp);
						nRows++;
					}
				}
			} catch (SQLException e) {
				LOGGER.error("Error inserting data to database.", e);
			}
			if (clear % 50 == 0) {
				insertedMap.clear();
			}
			LOGGER.info("Inserted " + nRows + " traffic speed band rows corresponding to batch: "
					+ clear + ", shall poll LTA web-service after five minutes");
			nRows = 0;

		}
	}
}
