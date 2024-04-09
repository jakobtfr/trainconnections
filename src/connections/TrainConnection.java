package pgdp.trains.connections;

import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public record TrainConnection(String trainName, String type, String line, String operator, List<TrainStop> stops) {

    public int totalTimeTraveledScheduled() {
        return totalTimeTraveledBy(TrainStop::scheduled);
    }

    public int totalTimeTraveledActual() {
        return totalTimeTraveledBy(TrainStop::actual);
    }

    public TrainStop getFirstStop() {
        return stops.get(0);
    }

    public TrainConnection withUpdatedStops(List<TrainStop> newStops) {
        return new TrainConnection(trainName, type, line, operator, newStops);
    }

    private int totalTimeTraveledBy(Function<TrainStop, LocalDateTime> mapper) {
        return stops.isEmpty() ? 0 : (int) ChronoUnit.MINUTES
                .between(mapper.apply(stops.get(0)), mapper.apply(stops.get(stops.size() - 1)));
    }

    /**
     * PARSING
     */
    public static TrainConnection parseFromJSON(JSONObject json) {
        JSONObject train = json.getJSONObject("train");
        String trainName = train.getString("name");
        String lineName = train.getString("line"); // or number
        String typeName = train.getString("type"); // or number
        JSONObject operator = train.getJSONObject("operator");
        String operatorName = operator.getString("name");
        List<TrainStop> trainStops = TrainStop.parseFromJSON(json.getJSONArray("stops"));
        return new TrainConnection(trainName, typeName, lineName, operatorName, trainStops);
    }

    /**
     * PARSING
     */
    public static Stream<TrainConnection> parseFromString(String content) {
        JSONArray array = new JSONArray(content);
        return IntStream.range(0, array.length()).mapToObj(array::getJSONObject).map(TrainConnection::parseFromJSON);
    }
}
