package pgdp.trains.connections;

import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.IntStream;

public record TrainStop(Station station, LocalDateTime scheduled, LocalDateTime actual, Kind kind) {

    public enum Kind {
        CANCELLED, ADDITIONAL, REGULAR
    }

    public int getDelay() {
        return (int) Math.max(ChronoUnit.MINUTES.between(scheduled, actual), 0);
    }

    /**
     * PARSING
     */
    private static final ZoneId ZONE_BERLIN = ZoneId.of("Europe/Berlin");
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZONE_BERLIN);

    /**
     * PARSING
     */
    public static TrainStop parseFromJSON(JSONObject json) {
        Station station = Station.parseFromJSON(json.getJSONObject("station"));
        JSONObject time;
        time = json.has("departure") ? json.getJSONObject("departure") : json.getJSONObject("arrival");
        LocalDateTime scheduled = LocalDateTime.parse(time.getString("scheduledTime"), FORMATTER);
        LocalDateTime actual = LocalDateTime.parse(time.getString("time"), FORMATTER);
        Kind kind = json.optBoolean("cancelled", false) ? Kind.CANCELLED : Kind.REGULAR;
        kind = json.optBoolean("additional", false) ? Kind.ADDITIONAL : kind;
        return new TrainStop(station, scheduled, actual, kind);
    }

    /**
     * PARSING
     */
    public static List<TrainStop> parseFromJSON(JSONArray jsonArray) {
        return IntStream.range(0, jsonArray.length()).mapToObj(jsonArray::getJSONObject)
                .map(TrainStop::parseFromJSON).toList();
    }
}
