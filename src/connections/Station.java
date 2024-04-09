package pgdp.trains.connections;

import org.json.JSONObject;

public record Station(long id, String name) {

    /**
     * TEST BAHNHÖFE - irrelevant für die Aufgabe!
     */
    public static final Station BERLIN_GESUNDBRUNNEN = new Station(8011102, "Berlin Gesundbrunnen");
    public static final Station BERLIN_HBF_TIEF = new Station(8098160, "Berlin Hbf (tief)");
    public static final Station BERLIN_SUEDKREUZ = new Station(8011113, "Berlin Südkreuz");
    public static final Station HALLE_SAALE_HBF = new Station(8010159, "Halle(Saale)Hbf");
    public static final Station ERFURT_HBF = new Station(8010101, "Erfurt Hbf");
    public static final Station NUERNBERG_HBF = new Station(8000284, "Nürnberg Hbf");
    public static final Station MUENCHEN_HBF = new Station(8000261, "München Hbf");
    public static final Station DORTMUND_HBF = new Station(8000080, "Dortmund Hbf");
    public static final Station BOCHUM_HBF = new Station(8000041, "Bochum Hbf");
    public static final Station ESSEN_HBF = new Station(8000098, "Essen Hbf");
    public static final Station DUISBURG_HBF = new Station(8000086, "Duisburg Hbf");
    public static final Station DUESSELDORF_HBF = new Station(8000085, "Düsseldorf Hbf");
    public static final Station KOELN_BONN_FLUGHAFEN = new Station(8003330, "Köln/Bonn Flughafen");
    public static final Station SIEGBURG_BONN = new Station(8005556, "Siegburg/Bonn");
    public static final Station MONTABAUR = new Station(8000667, "Montabaur");
    public static final Station LIMBURG_SUED = new Station(8003680, "Limburg Süd");
    public static final Station FRANKFURT_M__FLUGHAFEN_FERNBF = new Station(8070003, "Frankfurt(M) Flughafen Fernbf");
    public static final Station FRANKFURT_MAIN_HBF = new Station(8000105, "Frankfurt(Main)Hbf");
    public static final Station ASCHAFFENBURG_HBF = new Station(8000010, "Aschaffenburg Hbf");
    public static final Station WUERZBURG_HBF = new Station(8000260, "Würzburg Hbf");
    public static final Station KOELN_HBF = new Station(8000207, "Köln Hbf");
    public static final Station MANNHEIM_HBF = new Station(8000244, "Mannheim Hbf");
    public static final Station STUTTGART_HBF = new Station(8000096, "Stuttgart Hbf");
    public static final Station ULM_HBF = new Station(8000170, "Ulm Hbf");
    public static final Station AUGSBURG_HBF = new Station(8000013, "Augsburg Hbf");

    /**
     * PARSING - irrelevant für die Aufgabe!
     */
    public static Station parseFromJSON(JSONObject json) {
        long id = Long.parseLong(json.getString("id"));
        String name = json.getString("title");
        return new Station(id, name);
    }
}
