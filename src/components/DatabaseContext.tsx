import { createContext, useContext, useState, useEffect, useCallback, useRef, ReactNode } from 'react';
import * as duckdb from '@duckdb/duckdb-wasm';
import duckdbMvpWasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import duckdbMvpWorker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdbEhWasm from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import duckdbEhWorker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';

const DUCKDB_BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: duckdbMvpWasm,
    mainWorker: duckdbMvpWorker,
  },
  eh: {
    mainModule: duckdbEhWasm,
    mainWorker: duckdbEhWorker,
  },
};

interface Station {
  station_id: string;
  name_primary: string;
  name_latin?: string;
  lat: number;
  lon: number;
  country_code?: string;
  esr_code?: string;
  osm_node_id?: string;
  osm_way_id?: string;
  osm_relation_id?: string;
  wikidata_id?: string;
  wikipedia_ru?: string;
  parovoz_url?: string;
  railwayz_id?: string;
  current_status: string;
  geometry_quality?: string;
  notes?: string;
  created_at?: string;
  updated_at?: string;
}

interface StationName {
  station_id: string;
  name: string;
  language: string;
  valid_from?: string;
  valid_to?: string;
  name_type?: string;
  source_id?: string;
  notes?: string;
}

interface Event {
  event_id: string;
  event_type: string;
  date: string;
  date_precision?: string;
  line_id?: string;
  station_id?: string;
  segment_id?: string;
  description?: string;
  source_id?: string;
  source_page?: string;
  notes?: string;
}

interface Segment {
  segment_id: string;
  from_station_id: string;
  to_station_id: string;
  geometry: [number, number][];
  geometry_source?: string;
  geometry_quality?: string;
  is_current?: boolean;
  notes?: string;
}

interface DatabaseContextType {
  stations: Station[];
  stationNames: StationName[];
  events: Event[];
  segments: Segment[];
  isLoading: boolean;
  error: string | null;
  queryDataForYear: (year: number) => Promise<{ stations: StationWithState[]; segments: SegmentWithState[] }>;
}

interface StationWithState extends Station {
  state: 'planned' | 'existing' | 'new' | 'electrified' | 'gauge_change' | 'closed';
  alternative_names: { [key: string]: string };
}

interface SegmentWithState extends Segment {
  state: 'existing' | 'new' | 'electrified' | 'gauge_change' | 'closed';
}

const DatabaseContext = createContext<DatabaseContextType>({
  stations: [],
  stationNames: [],
  events: [],
  segments: [],
  isLoading: true,
  error: null,
  queryDataForYear: async () => ({ stations: [], segments: [] }),
});

export const useDatabase = () => useContext(DatabaseContext);

interface DatabaseProviderProps {
  children: ReactNode;
}

const buildAltNameMap = (names: any): { [key: string]: string } => {
  if (!names || !Array.isArray(names)) return {};

  const altNames: Record<string, string> = {};
  const langCounts: Record<string, number> = {};

  names.forEach(entry => {
    const name = (entry as any)?.name;
    const language = (entry as any)?.language;
    if (!name || !language) return;

    langCounts[language] = (langCounts[language] ?? 0) + 1;
    const suffix = langCounts[language] > 1 ? `_${langCounts[language] - 1}` : '';
    altNames[`name:${language}${suffix}`] = name;
  });

  return altNames;
};

const normalizeGeometry = (raw: any): [number, number][] => {
  if (!raw) return [];

  // Accept JSON-stringified geometry arrays or Feature-like payloads.
  let data: any = raw;
  if (typeof raw === 'string') {
    try {
      data = JSON.parse(raw);
    } catch {
      return [];
    }
  }

  // Allow GeoJSON Feature/FeatureCollection/Geometry objects by pulling coordinates.
  if (data && typeof data === 'object' && !Array.isArray(data)) {
    if (data.type === 'Feature' && data.geometry) {
      data = data.geometry.coordinates;
    } else if (data.type === 'FeatureCollection' && Array.isArray(data.features) && data.features[0]?.geometry) {
      data = data.features[0].geometry.coordinates;
    } else if (data.type && data.coordinates) {
      data = data.coordinates;
    }
  }

  if (!Array.isArray(data)) return [];

  return data
    .map((pair: any) => {
      if (Array.isArray(pair)) {
        return [Number(pair[0]), Number(pair[1])] as [number, number];
      }
      if (pair && typeof pair === 'object') {
        if ('f0' in pair && 'f1' in pair) {
          return [Number((pair as any).f0), Number((pair as any).f1)] as [number, number];
        }
        if ('lat' in pair && 'lon' in pair) {
          return [Number((pair as any).lat), Number((pair as any).lon)] as [number, number];
        }
      }
      return [NaN, NaN] as [number, number];
    })
    .filter(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon));
};

const DEMO_EVENTS: Event[] = [
  { event_id: 'EVT_0001', event_type: 'station_open', date: '1851-11-01', date_precision: 'month', station_id: 'RU.STN.Moskva-Passazhirskaya-Paveletskaya', description: 'Moscow Passenger opened' },
  { event_id: 'EVT_0002', event_type: 'station_open', date: '1851-11-01', date_precision: 'month', station_id: 'RU.STN.Sankt-Peterburg-Baltiyskiy', description: 'Saint Petersburg opened' },
  { event_id: 'EVT_0003', event_type: 'station_open', date: '1903-07-21', date_precision: 'day', station_id: 'RU.STN.Vladivostok', description: 'Vladivostok opened' },
  { event_id: 'EVT_0004', event_type: 'station_open', date: '1878-05-01', date_precision: 'month', station_id: 'RU.STN.Ekaterinburg-Passazhirskiy', description: 'Yekaterinburg opened' },
  { event_id: 'EVT_0005', event_type: 'station_open', date: '1893-04-01', date_precision: 'month', station_id: 'RU.STN.Novosibirsk-Glavnyy', description: 'Novosibirsk opened' },
  { event_id: 'EVT_0006', event_type: 'station_open', date: '1898-08-16', date_precision: 'day', station_id: 'RU.STN.Irkutsk-Passazhirskiy', description: 'Irkutsk closed' },
  { event_id: 'EVT_0007', event_type: 'station_close', date: '1975-06-01', date_precision: 'month', station_id: 'RU.STN.Vladivostok', description: 'Vladivostok closed' },
  { event_id: 'EVT_0008', event_type: 'electrification', date: '1935-12-15', date_precision: 'day', station_id: 'RU.STN.Moskva-Passazhirskaya-Paveletskaya', description: 'Moscow electrified' },
  { event_id: 'EVT_0009', event_type: 'electrification', date: '1936-01-10', date_precision: 'day', station_id: 'RU.STN.Sankt-Peterburg-Baltiyskiy', description: 'Saint Petersburg electrified' },
  { event_id: 'EVT_0010', event_type: 'station_open', date: '1860-01-01', date_precision: 'year', station_id: 'STN_0007', description: 'Mock station opened' },
  { event_id: 'EVT_0011', event_type: 'station_open', date: '1896-09-12', date_precision: 'day', station_id: 'RU.STN.Kazan-Passazhirskaya', description: 'Kazan opened' },
  { event_id: 'EVT_SEG_0001', event_type: 'segment_open', date: '1851-11-01', date_precision: 'month', segment_id: 'SEG_0001', description: 'Moscow-Petersburg segment opened' },
  { event_id: 'EVT_SEG_0002', event_type: 'segment_open', date: '1916-10-05', date_precision: 'day', segment_id: 'SEG_0002', description: 'Trans-Siberian segment opened' },
  { event_id: 'EVT_SEG_0003', event_type: 'segment_open', date: '1916-10-05', date_precision: 'day', segment_id: 'SEG_0003', description: 'Trans-Siberian segment opened' },
  { event_id: 'EVT_SEG_0004', event_type: 'segment_open', date: '1896-10-01', date_precision: 'month', segment_id: 'SEG_0004', description: 'Trans-Siberian segment opened' },
  { event_id: 'EVT_SEG_0005', event_type: 'segment_open', date: '1898-08-16', date_precision: 'day', segment_id: 'SEG_0005', description: 'Trans-Siberian segment opened' },
  { event_id: 'EVT_SEG_0006', event_type: 'segment_open', date: '1900-01-01', date_precision: 'year', segment_id: 'SEG_0006', description: 'Northern segment opened' },
  { event_id: 'EVT_SEG_0007', event_type: 'segment_open', date: '1902-01-01', date_precision: 'year', segment_id: 'SEG_0007', description: 'Western connection opened' },
  { event_id: 'EVT_SEG_0008', event_type: 'electrification', date: '1935-12-15', date_precision: 'day', segment_id: 'SEG_0001', description: 'Moscow-Petersburg electrified' },
  { event_id: 'EVT_SEG_0009', event_type: 'segment_close', date: '1975-06-01', date_precision: 'month', segment_id: 'SEG_0003', description: 'Vladivostok segment closed' },
];

const DEMO_SEGMENTS: Segment[] = [
  { segment_id: 'SEG_0001', from_station_id: 'RU.STN.Moskva-Passazhirskaya-Paveletskaya', to_station_id: 'RU.STN.Sankt-Peterburg-Baltiyskiy', geometry: [[55.7765, 37.6550], [59.9311, 30.3609]], geometry_quality: 'high' },
  { segment_id: 'SEG_0002', from_station_id: 'RU.STN.Kazan-Passazhirskaya', to_station_id: 'RU.STN.Ekaterinburg-Passazhirskiy', geometry: [[55.7887, 49.1221], [56.8519, 60.6122]], geometry_quality: 'high' },
  { segment_id: 'SEG_0003', from_station_id: 'RU.STN.Vladivostok', to_station_id: 'RU.STN.Irkutsk-Passazhirskiy', geometry: [[43.1056, 131.8735], [52.2869, 104.3050]], geometry_quality: 'medium' },
  { segment_id: 'SEG_0004', from_station_id: 'RU.STN.Ekaterinburg-Passazhirskiy', to_station_id: 'RU.STN.Novosibirsk-Glavnyy', geometry: [[56.8519, 60.6122], [55.0415, 82.9346]], geometry_quality: 'high' },
  { segment_id: 'SEG_0005', from_station_id: 'RU.STN.Novosibirsk-Glavnyy', to_station_id: 'RU.STN.Irkutsk-Passazhirskiy', geometry: [[55.0415, 82.9346], [52.2869, 104.3050]], geometry_quality: 'medium' },
  { segment_id: 'SEG_0006', from_station_id: 'RU.STN.Irkutsk-Passazhirskiy', to_station_id: 'STN_0007', geometry: [[52.2869, 104.3050], [60.0, 100.0]], geometry_quality: 'low' },
  { segment_id: 'SEG_0007', from_station_id: 'RU.STN.Moskva-Passazhirskaya-Paveletskaya', to_station_id: 'RU.STN.Kazan-Passazhirskaya', geometry: [[55.7290973,37.6408132], [55.7887, 49.1221]], geometry_quality: 'high' },
];

export function DatabaseProvider({ children }: DatabaseProviderProps) {
  const [stations, setStations] = useState<Station[]>([]);
  const [stationNames, setStationNames] = useState<StationName[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [segments, setSegments] = useState<Segment[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const connectionRef = useRef<duckdb.AsyncDuckDBConnection | null>(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        setIsLoading(true);
        setError(null);

        const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
        const worker = new Worker(bundle.mainWorker!);
        const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR);
        const db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

        const conn = await db.connect();
        connectionRef.current = conn;

        // Relax expression depth to avoid limits when aggregating JSON/structs
        await conn.query(`SET max_expression_depth TO 5000;`);

        const base = (import.meta as any).env?.BASE_URL || '/';
        const [stationsRes, stationNamesRes] = await Promise.all([
          fetch(`${base}data/stations.parquet`),
          fetch(`${base}data/station_names.parquet`),
        ]);

        if (!stationsRes.ok) {
          throw new Error(`Failed to load stations.parquet (${stationsRes.status})`);
        }
        if (!stationNamesRes.ok) {
          throw new Error(`Failed to load station_names.parquet (${stationNamesRes.status})`);
        }

        const [stationsBuffer, stationNamesBuffer] = await Promise.all([
          stationsRes.arrayBuffer(),
          stationNamesRes.arrayBuffer(),
        ]);

        await db.registerFileBuffer('stations.parquet', new Uint8Array(stationsBuffer));
        await db.registerFileBuffer('station_names.parquet', new Uint8Array(stationNamesBuffer));

        await conn.query(`CREATE OR REPLACE TABLE stations AS SELECT * FROM read_parquet('stations.parquet');`);
        await conn.query(`CREATE OR REPLACE TABLE station_names AS SELECT * FROM read_parquet('station_names.parquet');`);

        await db.registerFileText('events.json', JSON.stringify(DEMO_EVENTS));
        await db.registerFileText('segments.json', JSON.stringify(DEMO_SEGMENTS));
        await conn.query(`CREATE OR REPLACE TABLE events AS SELECT * FROM read_json_auto('events.json');`);
        await conn.query(`CREATE OR REPLACE TABLE segments AS SELECT * FROM read_json_auto('segments.json');`);

        const stationCount = await conn.query(`SELECT COUNT(*) AS cnt FROM stations;`);
        const segmentCount = await conn.query(`SELECT COUNT(*) AS cnt FROM segments;`);

        const stationsTable = await conn.query(`SELECT * FROM stations WHERE lat IS NOT NULL AND lon IS NOT NULL;`);
        const stationNamesTable = await conn.query(`SELECT * FROM station_names WHERE station_id IS NOT NULL AND name IS NOT NULL AND language IS NOT NULL;`);

        const loadedStations: Station[] = stationsTable.toArray().map((row: any) => ({
          station_id: String(row.station_id),
          name_primary: row.name_primary || String(row.station_id),
          name_latin: row.name_latin || undefined,
          lat: Number(row.lat),
          lon: Number(row.lon),
          country_code: row.country_code || undefined,
          esr_code: row.esr_code || undefined,
          osm_node_id: row.osm_node_id || undefined,
          osm_way_id: row.osm_way_id || undefined,
          osm_relation_id: row.osm_relation_id || undefined,
          wikidata_id: row.wikidata_id || undefined,
          wikipedia_ru: row.wikipedia_ru || undefined,
          parovoz_url: row.parovoz_url || undefined,
          railwayz_id: row.railwayz_id || undefined,
          current_status: row.current_status || 'open',
          geometry_quality: row.geometry_quality || undefined,
          notes: row.notes || undefined,
          created_at: row.created_at || undefined,
          updated_at: row.updated_at || undefined,
        }));

        const loadedStationNames: StationName[] = stationNamesTable.toArray().map((row: any) => ({
          station_id: String(row.station_id),
          name: row.name,
          language: row.language,
          valid_from: row.valid_from || undefined,
          valid_to: row.valid_to || undefined,
          name_type: row.name_type || undefined,
          source_id: row.source_id || undefined,
          notes: row.notes || undefined,
        }));

        setStations(loadedStations);
        setStationNames(loadedStationNames);
        setEvents(DEMO_EVENTS);
        setSegments(DEMO_SEGMENTS);
      } catch (err: any) {
        setError(err?.message || 'Failed to initialize DuckDB');
      } finally {
        setIsLoading(false);
      }
    };

    loadData();

    return () => {
      (async () => {
        await connectionRef.current?.close();
        connectionRef.current = null;
      })();
    };
  }, []);

  const queryDataForYear = useCallback(
    async (year: number): Promise<{ stations: StationWithState[]; segments: SegmentWithState[] }> => {
      const conn = connectionRef.current;
      if (!conn) {
        return { stations: [], segments: [] };
      }

      const stationsForYearTable = await conn.query(`
        WITH station_events AS (
          SELECT
            s.*,
            MAX(CASE WHEN e.event_type = 'station_open' THEN EXTRACT(YEAR FROM CAST(e.date AS DATE)) END) AS open_year,
            MAX(CASE WHEN e.event_type = 'station_close' THEN EXTRACT(YEAR FROM CAST(e.date AS DATE)) END) AS close_year,
            MAX(CASE WHEN e.event_type = 'electrification' AND EXTRACT(YEAR FROM CAST(e.date AS DATE)) <= ${year} THEN EXTRACT(YEAR FROM CAST(e.date AS DATE)) END) AS electrified_year
          FROM stations s
          LEFT JOIN events e ON s.station_id = e.station_id
          WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
          GROUP BY ALL
        ),
        station_state AS (
          SELECT
            se.*,
            COALESCE(se.open_year, CAST(EXTRACT(YEAR FROM CAST(se.created_at AS DATE)) AS INTEGER), 99999) AS effective_open_year,
            CASE
              WHEN se.open_year IS NULL AND se.created_at IS NULL THEN 'planned'
              WHEN COALESCE(se.open_year, CAST(EXTRACT(YEAR FROM CAST(se.created_at AS DATE)) AS INTEGER), 99999) > ${year} THEN 'planned'
              WHEN se.close_year IS NOT NULL AND se.close_year < ${year} THEN NULL
              WHEN se.close_year IS NOT NULL AND se.close_year = ${year} THEN 'closed'
              WHEN se.current_status = 'closed' THEN 'closed'
              WHEN se.electrified_year = ${year} THEN 'electrified'
              WHEN se.open_year = ${year} THEN 'new'
              ELSE 'existing'
            END AS state_label
          FROM station_events se
        ),
        station_names_agg AS (
          SELECT
            station_id,
            list(name) AS names,
            list(language) AS languages
          FROM station_names
          WHERE station_id IS NOT NULL AND name IS NOT NULL AND language IS NOT NULL
          GROUP BY station_id
        )
        SELECT ss.*, sna.names, sna.languages
        FROM station_state ss
        LEFT JOIN station_names_agg sna ON ss.station_id = sna.station_id
        WHERE ss.state_label IS NOT NULL;
      `);

      const stationsForYearRows = stationsForYearTable.toArray();

      const stationsForYear: StationWithState[] = stationsForYearRows.map((row: any) => ({
        station_id: String(row.station_id),
        name_primary: row.name_primary || String(row.station_id),
        name_latin: row.name_latin || undefined,
        lat: Number(row.lat),
        lon: Number(row.lon),
        country_code: row.country_code || undefined,
        esr_code: row.esr_code || undefined,
        osm_node_id: row.osm_node_id || undefined,
        osm_way_id: row.osm_way_id || undefined,
        osm_relation_id: row.osm_relation_id || undefined,
        wikidata_id: row.wikidata_id || undefined,
        wikipedia_ru: row.wikipedia_ru || undefined,
        parovoz_url: row.parovoz_url || undefined,
        railwayz_id: row.railwayz_id || undefined,
        current_status: row.current_status || 'open',
        geometry_quality: row.geometry_quality || undefined,
        notes: row.notes || undefined,
        created_at: row.created_at || undefined,
        updated_at: row.updated_at || undefined,
        state: row.state_label as StationWithState['state'],
        alternative_names: buildAltNameMap(
          Array.isArray(row.names) && Array.isArray(row.languages)
            ? row.names.map((n: any, idx: number) => ({ name: n, language: row.languages[idx] }))
            : row.names
        ),
      }));

      const segmentsForYearTable = await conn.query(`
        WITH base AS (
          SELECT * FROM segments
        ),
        open_years AS (
          SELECT segment_id, MIN(EXTRACT(YEAR FROM CAST(date AS DATE))) AS open_year
          FROM events
          WHERE segment_id IS NOT NULL AND event_type = 'segment_open'
          GROUP BY segment_id
        ),
        close_years AS (
          SELECT segment_id, MIN(EXTRACT(YEAR FROM CAST(date AS DATE))) AS close_year
          FROM events
          WHERE segment_id IS NOT NULL AND event_type = 'segment_close'
          GROUP BY segment_id
        ),
        electrified_years AS (
          SELECT segment_id, MAX(EXTRACT(YEAR FROM CAST(date AS DATE))) AS electrified_year
          FROM events
          WHERE segment_id IS NOT NULL AND event_type = 'electrification' AND EXTRACT(YEAR FROM CAST(date AS DATE)) <= ${year}
          GROUP BY segment_id
        )
        SELECT
          b.segment_id,
          b.from_station_id,
          b.to_station_id,
          CAST(b.geometry AS VARCHAR) AS geometry_json,
          b.geometry_quality,
          o.open_year,
          c.close_year,
          el.electrified_year,
          CASE
            WHEN ${year} < COALESCE(o.open_year, 0) THEN 'planned'
            WHEN c.close_year IS NOT NULL AND c.close_year < ${year} THEN NULL
            WHEN c.close_year IS NOT NULL AND c.close_year = ${year} THEN 'closed'
            WHEN el.electrified_year = ${year} THEN 'electrified'
            WHEN o.open_year IS NOT NULL AND o.open_year = ${year} THEN 'new'
            ELSE 'existing'
          END AS state_label
        FROM base b
        LEFT JOIN open_years o USING (segment_id)
        LEFT JOIN close_years c USING (segment_id)
        LEFT JOIN electrified_years el USING (segment_id);
      `);

      const segmentRows = segmentsForYearTable.toArray();

      const segmentsForYear: SegmentWithState[] = segmentRows
        .filter((row: any) => Boolean(row.state_label))
        .map((row: any) => ({
          segment_id: String(row.segment_id),
          from_station_id: String(row.from_station_id),
          to_station_id: String(row.to_station_id),
          geometry: normalizeGeometry(row.geometry_json ?? row.geometry),
          geometry_source: row.geometry_source || undefined,
          geometry_quality: row.geometry_quality || undefined,
          is_current: row.is_current || undefined,
          notes: row.notes || undefined,
          state: row.state_label as SegmentWithState['state'],
        }));

      return { stations: stationsForYear, segments: segmentsForYear };
    },
    [],
  );

  return (
    <DatabaseContext.Provider value={{ stations, stationNames, events, segments, isLoading, error, queryDataForYear }}>
      {children}
    </DatabaseContext.Provider>
  );
}
