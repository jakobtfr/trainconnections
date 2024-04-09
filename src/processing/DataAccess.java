package pgdp.trains.processing;

import org.json.JSONException;
import pgdp.trains.connections.Station;
import pgdp.trains.connections.TrainConnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataAccess {

    private static final String URL_DEPARTURE_BOARD = "https://bahn.expert/api/hafas/v2/arrivalStationBoard/?station=";
    private static final ZoneId ZONE_BERLIN = ZoneId.of("Europe/Berlin");
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZONE_BERLIN);

    public static Stream<TrainConnection> getDepartureBoardFor(Station station, LocalDateTime date) {
        BufferedReader in;
        try {
            String urlBuild = URL_DEPARTURE_BOARD + station.id() + (date != null ? "&date=" + date.format(FORMATTER) : "");
            URL url = new URL(urlBuild);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String content = in.lines().collect(Collectors.joining());
            Files.writeString(Path.of("connections_test/" + (date != null ? date.format(FORMATTER) :
                    "con" + Math.random()) + ".json"), content);
            return TrainConnection.parseFromString(content);
        } catch (MalformedURLException mfE) {
            System.err.println("URL hat kein gültiges Format.");
        } catch (IOException ioE) {
            System.err.println("Fehler beim öffnen/lesen von der Website");
        } catch (JSONException jsonE) {
            System.err.println("Fehler beim parsen zu JSON.");
        }
        return Stream.empty();
    }

    public static Stream<TrainConnection> getDepartureBoardNowFor(Station station) {
        return getDepartureBoardFor(station, null);
    }

    public static Stream<TrainConnection> loadFile(String pathToFile) {
        try {
            String content = Files.readString(Path.of(pathToFile));
            return TrainConnection.parseFromString(content);
        } catch (IOException e) {
            System.err.println("Es konnte nicht aus der Datei gelesen werden!");
            return Stream.empty();
        }
    }
}
