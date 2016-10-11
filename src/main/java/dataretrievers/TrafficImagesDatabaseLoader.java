package dataretrievers;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import utils.DatabaseAccess;

public class TrafficImagesDatabaseLoader extends DatabaseLoader<JSONObject> {

	private int clear = 0;
	private List<Integer> insertedCodeList;
	private static final Logger LOGGER = Logger
			.getLogger(TrafficImagesDatabaseLoader.class);

	private static final DateTimeFormatter df = DateTimeFormat
			.forPattern("yyyy-MM-dd'T'HH:mm:ss");

	public TrafficImagesDatabaseLoader(Queue<JSONObject> xmlDocQueue,
			DatabaseAccess access) {
		super(xmlDocQueue, access);
		insertedCodeList = new ArrayList<Integer>();
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
			int nRows = 0;
			try {
				JSONObject data = xmlDocQueue.poll();
				clear++;

				JSONArray imageSet = (JSONArray) data.get("value");
				Iterator<JSONObject> it = imageSet.iterator();

				while (it.hasNext()) {
					JSONObject element = it.next();
					URL imageUrl = new URL((String) element.get("ImageLink"));
					URLConnection con = imageUrl.openConnection();
				    con.setReadTimeout( 5000 ); //2 seconds
				    InputStream is =con.getInputStream();
					BufferedImage  image = ImageIO.read(is);
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					ImageIO.write(image,"jpg", baos);
					byte[] imageBytes = baos.toByteArray();
					baos.close();
					is.close();
					int imageCode = Arrays.hashCode(imageBytes);
					if (!insertedCodeList.contains(imageCode)) {
						double latitude = (double) element.get("Latitude");
						double longitude = (double) element.get("Longitude");
						int cameraId = Integer
								.parseInt((String) element.get("CameraID"));
						PreparedStatement ps = access.getPreparedStatement(
								"INSERT INTO  ltaimageset VALUES (?,?,?,?,?)");

						ps.setDouble(1, latitude);
						ps.setDouble(2, longitude);
						ps.setInt(3, cameraId);
						ps.setLong(4, System.currentTimeMillis());
						ps.setBytes(5, imageBytes);
						ps.execute();
						nRows++;
						if (insertedCodeList.size() < 2000) {
							insertedCodeList.add(imageCode);
						} else {
							insertedCodeList.set(clear, imageCode);
							if (clear == 2000)
								clear = 0;
							else
								clear++;
						}

						ps.close();
					}

				}

			} catch (SQLException e) {
				LOGGER.error("Error inserting data to database.", e);
			} catch (MalformedURLException e) {
				LOGGER.error("Error reading image from an invalid  URL", e);
			} catch (IOException e) {
				LOGGER.error("Error reading image from the  URL", e);
			}catch(Exception ex){
				ex.getCause().printStackTrace();
			}

			LOGGER.info("Inserted " + nRows
					+ " images shall poll LTA web-service after one minutes");
			nRows = 0;

		}

	}

}
