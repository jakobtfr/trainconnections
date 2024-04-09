package pgdp.trains.processing;

import pgdp.trains.connections.Station;
import pgdp.trains.connections.TrainConnection;
import pgdp.trains.connections.TrainStop;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataProcessing {

    public static Stream<TrainConnection> cleanDataset(Stream<TrainConnection> connections) {
        return connections.distinct()
                .sorted(Comparator.comparing(tc -> tc.getFirstStop().scheduled()))
                .map(c -> c.withUpdatedStops(c.stops().stream().filter(trainStop -> !trainStop.kind().equals(TrainStop.Kind.CANCELLED)).toList()));
    }

    public static TrainConnection worstDelayedTrain(Stream<TrainConnection> connections) {
        return connections.max(Comparator.comparing(c ->
                c.stops().stream().mapToInt(TrainStop::getDelay).max().orElse(0))).orElse(null);
    }

    public static double percentOfKindStops(Stream<TrainConnection> connections, TrainStop.Kind kind) {
        return connections.flatMap(c -> c.stops().stream())
                .mapToDouble(trainStop -> {
                    if (trainStop.kind().equals(kind)) {
                        return 1;
                    } else {
                        return 0;
                    }
                }).average().orElse(0.0) * 100;
    }

    public static double averageDelayAt(Stream<TrainConnection> connections, Station station) {
        return connections.flatMap(tc -> tc.stops().stream())
                .filter(stop -> stop.station().equals(station))
                .mapToInt(TrainStop::getDelay).average()
                .orElse(0.0);
    }

    public static Map<String, Double> delayComparedToTotalTravelTimeByTransport(Stream<TrainConnection> connections) {
        Map<String, List<TrainConnection>> type = connections.collect(Collectors.groupingBy(TrainConnection::type));


        return type.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, tc ->
                {
                    double scheduled = tc.getValue().stream().mapToDouble(TrainConnection::totalTimeTraveledScheduled).sum();
                    double delay = tc.getValue().stream().mapToDouble(TrainConnection::totalTimeTraveledActual).sum();
                    return ((delay-scheduled) / delay) * 100;
                }));

    }

    public static Map<Integer, Double> averageDelayByHour(Stream<TrainConnection> connections) {
        Map <Integer, List<TrainStop>> time = connections
                .flatMap(tc -> tc.stops().stream())
                .collect(Collectors.groupingBy(ts -> ts.actual().getHour()));

        return time.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, ts ->
                ts.getValue().stream().mapToDouble(TrainStop::getDelay).average().orElse(0.0)
        ));
    }

    public static void main(String[] args) {

        List<TrainConnection> trainConnections = List.of(
                new TrainConnection("ICE 2", "ICE", "2", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 11, 0),
                                LocalDateTime.of(2022, 12, 1, 11, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 11, 30),
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                TrainStop.Kind.REGULAR)
                )),
                new TrainConnection("ICE 1", "ICE", "1", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 10, 0),
                                LocalDateTime.of(2022, 12, 1, 10, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 10, 30),
                                LocalDateTime.of(2022, 12, 1, 10, 30),
                                TrainStop.Kind.REGULAR)
                )),
                new TrainConnection("ICE 3", "ICE", "3", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.AUGSBURG_HBF,
                                LocalDateTime.of(2022, 12, 1, 12, 20),
                                LocalDateTime.of(2022, 12, 1, 13, 0),
                                TrainStop.Kind.CANCELLED),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 13, 30),
                                LocalDateTime.of(2022, 12, 1, 13, 30),
                                TrainStop.Kind.REGULAR)
                ))
        );

        List<TrainConnection> cleanDataset = cleanDataset(trainConnections.stream()).toList();
        // cleanDataset sollte sortiert sein: [ICE 1, ICE 2, ICE 3] und bei ICE 3 sollte der Stopp in AUGSBURG_HBF
        // nicht mehr enthalten sein.

        TrainConnection worstDelayedTrain = worstDelayedTrain(trainConnections.stream());
        // worstDelayedTrain sollte ICE 3 sein. (Da der Stop in AUGSBURG_HBF mit 40 Minuten Verspätung am spätesten ist.)

        double percentOfKindStops = percentOfKindStops(trainConnections.stream(), TrainStop.Kind.CANCELLED);
        // percentOfKindStops REGULAR sollte 85.71428571428571 sein, CANCELLED 14.285714285714285.

        double averageDelayAt = averageDelayAt(trainConnections.stream(), Station.NUERNBERG_HBF);
        // averageDelayAt sollte 10.0 sein. (Da dreimal angefahren und einmal 30 Minuten Verspätung).

        Map<String, Double> delayCompared = delayComparedToTotalTravelTimeByTransport(trainConnections.stream());
        // delayCompared sollte ein Map sein, die für ICE den Wert 16.666666666666668 hat.
        // Da ICE 2 0:30 geplant hatte, aber 1:00 gebraucht hat, ICE 1 0:30 geplant und gebraucht hatte, und
        // ICE 3 1:30 geplant und gebraucht hat. Zusammen also 2:30 geplant und 3:00 gebraucht, und damit
        // (3:00 - 2:30) / 3:00 = 16.666666666666668.

        Map<Integer, Double> averageDelayByHourOfDay = averageDelayByHour(trainConnections.stream());
        // averageDelayByHourOfDay sollte ein Map sein, die für 10, 11 den Wert 0.0 hat, für 12 den Wert 15.0 und
        // für 13 den Wert 20.0.

    }


}
