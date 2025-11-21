import { createContext, useContext, useState, useEffect, ReactNode } from 'react';

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
  queryDataForYear: (year: number) => { stations: StationWithState[]; segments: SegmentWithState[] };
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
  queryDataForYear: () => ({ stations: [], segments: [] }),
});

export const useDatabase = () => useContext(DatabaseContext);

interface DatabaseProviderProps {
  children: ReactNode;
}

export function DatabaseProvider({ children }: DatabaseProviderProps) {
  const [stations, setStations] = useState<Station[]>([]);
  const [stationNames, setStationNames] = useState<StationName[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [segments, setSegments] = useState<Segment[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Minimal CSV parser that supports quoted fields
  const parseCSV = (text: string): string[][] => {
    const rows: string[][] = [];
    let currentRow: string[] = [];
    let currentValue = '';
    let inQuotes = false;

    for (let i = 0; i < text.length; i++) {
      const char = text[i];
      if (inQuotes) {
        if (char === '"') {
          if (text[i + 1] === '"') {
            currentValue += '"';
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          currentValue += char;
        }
      } else {
        if (char === '"') {
          inQuotes = true;
        } else if (char === ',') {
          currentRow.push(currentValue);
          currentValue = '';
        } else if (char === '\n') {
          currentRow.push(currentValue);
          rows.push(currentRow);
          currentRow = [];
          currentValue = '';
        } else if (char === '\r') {
          continue;
        } else {
          currentValue += char;
        }
      }
    }

    if (currentValue || currentRow.length) {
      currentRow.push(currentValue);
      rows.push(currentRow);
    }

    return rows.filter(row => row.some(cell => cell.trim() !== ''));
  };

  const parseCSVToObjects = (text: string): Record<string, string>[] => {
    const rows = parseCSV(text);
    if (!rows.length) return [];
    const headers = rows[0].map(h => h.trim());
    return rows.slice(1).map(row => {
      const obj: Record<string, string> = {};
      headers.forEach((header, idx) => {
        obj[header] = (row[idx] ?? '').trim();
      });
      return obj;
    });
  };

  useEffect(() => {
    const loadData = async () => {
      try {
        setIsLoading(true);
        setError(null);

        const base = (import.meta as any).env?.BASE_URL || '/';

        const [stationsRes, stationNamesRes] = await Promise.all([
          fetch(`${base}data/stations.csv`),
          fetch(`${base}data/station_names.csv`),
        ]);

        if (!stationsRes.ok) {
          throw new Error(`Failed to load stations.csv (${stationsRes.status})`);
        }
        if (!stationNamesRes.ok) {
          throw new Error(`Failed to load station_names.csv (${stationNamesRes.status})`);
        }

        const [stationsText, stationNamesText] = await Promise.all([
          stationsRes.text(),
          stationNamesRes.text(),
        ]);

        const stationRecords = parseCSVToObjects(stationsText);
        const stationNameRecords = parseCSVToObjects(stationNamesText);

        const loadedStations: Station[] = stationRecords
          .filter(rec => rec.station_id && rec.lat && rec.lon)
          .map(rec => ({
            station_id: rec.station_id,
            name_primary: rec.name_primary || rec.station_id,
            name_latin: rec.name_latin || undefined,
            lat: parseFloat(rec.lat),
            lon: parseFloat(rec.lon),
            country_code: rec.country_code || undefined,
            esr_code: rec.esr_code || undefined,
            osm_node_id: rec.osm_node_id || undefined,
            osm_way_id: rec.osm_way_id || undefined,
            osm_relation_id: rec.osm_relation_id || undefined,
            wikidata_id: rec.wikidata_id || undefined,
            wikipedia_ru: rec.wikipedia_ru || undefined,
            parovoz_url: rec.parovoz_url || undefined,
            railwayz_id: rec.railwayz_id || undefined,
            current_status: rec.current_status || 'open',
            geometry_quality: rec.geometry_quality || undefined,
            notes: rec.notes || undefined,
            created_at: rec.created_at || undefined,
            updated_at: rec.updated_at || undefined,
          }));

        const loadedStationNames: StationName[] = stationNameRecords
          .filter(rec => rec.station_id && rec.name && rec.language)
          .map(rec => ({
            station_id: rec.station_id,
            name: rec.name,
            language: rec.language,
            valid_from: rec.valid_from || undefined,
            valid_to: rec.valid_to || undefined,
            name_type: rec.name_type || undefined,
            source_id: rec.source_id || undefined,
            notes: rec.notes || undefined,
          }));

        // Keep mock timelines/segments so the map still has demo flows until real event/segment CSVs exist.
        const demoEvents: Event[] = [
          { event_id: 'EVT_0001', event_type: 'station_open', date: '1851-11-01', date_precision: 'month', station_id: 'RU.STN.Moskva-Passazhirskaya-Paveletskaya', description: 'Moscow Passenger opened' },
          { event_id: 'EVT_0002', event_type: 'station_open', date: '1851-11-01', date_precision: 'month', station_id: 'RU.STN.Sankt-Peterburg-Baltiyskiy', description: 'Saint Petersburg opened' },
          { event_id: 'EVT_0003', event_type: 'station_open', date: '1903-07-21', date_precision: 'day', station_id: 'RU.STN.Vladivostok', description: 'Vladivostok opened' },
          { event_id: 'EVT_0004', event_type: 'station_open', date: '1878-05-01', date_precision: 'month', station_id: 'RU.STN.Ekaterinburg-Passazhirskiy', description: 'Yekaterinburg opened' },
          { event_id: 'EVT_0005', event_type: 'station_open', date: '1893-04-01', date_precision: 'month', station_id: 'RU.STN.Novosibirsk-Glavnyy', description: 'Novosibirsk opened' },
          { event_id: 'EVT_0006', event_type: 'station_open', date: '1898-08-16', date_precision: 'day', station_id: 'RU.STN.Irkutsk-Passazhirskiy', description: 'Irkutsk opened' },
          { event_id: 'EVT_0007', event_type: 'station_close', date: '1975-06-01', date_precision: 'month', station_id: 'RU.STN.Irkutsk-Passazhirskiy', description: 'Irkutsk closed' },
          { event_id: 'EVT_0008', event_type: 'electrification', date: '1935-12-15', date_precision: 'day', station_id: 'RU.STN.Moskva-Passazhirskaya-Paveletskaya', description: 'Moscow electrified' },
          { event_id: 'EVT_0009', event_type: 'electrification', date: '1936-01-10', date_precision: 'day', station_id: 'RU.STN.Sankt-Peterburg-Baltiyskiy', description: 'Saint Petersburg electrified' },
          { event_id: 'EVT_0010', event_type: 'station_open', date: '1860-01-01', date_precision: 'year', station_id: 'STN_0007', description: 'Mock station opened' },
          { event_id: 'EVT_0011', event_type: 'station_open', date: '1896-09-12', date_precision: 'day', station_id: 'STN_0008', description: 'Kazan opened' },
          { event_id: 'EVT_SEG_0001', event_type: 'segment_open', date: '1851-11-01', date_precision: 'month', segment_id: 'SEG_0001', description: 'Moscow-Petersburg segment opened' },
          { event_id: 'EVT_SEG_0002', event_type: 'segment_open', date: '1916-10-05', date_precision: 'day', segment_id: 'SEG_0002', description: 'Trans-Siberian segment opened' },
          { event_id: 'EVT_SEG_0003', event_type: 'segment_open', date: '1916-10-05', date_precision: 'day', segment_id: 'SEG_0003', description: 'Trans-Siberian segment opened' },
          { event_id: 'EVT_SEG_0004', event_type: 'segment_open', date: '1896-10-01', date_precision: 'month', segment_id: 'SEG_0004', description: 'Trans-Siberian segment opened' },
          { event_id: 'EVT_SEG_0005', event_type: 'segment_open', date: '1898-08-16', date_precision: 'day', segment_id: 'SEG_0005', description: 'Trans-Siberian segment opened' },
          { event_id: 'EVT_SEG_0006', event_type: 'segment_open', date: '1900-01-01', date_precision: 'year', segment_id: 'SEG_0006', description: 'Northern segment opened' },
          { event_id: 'EVT_SEG_0007', event_type: 'segment_open', date: '1902-01-01', date_precision: 'year', segment_id: 'SEG_0007', description: 'Western connection opened' },
          { event_id: 'EVT_SEG_0008', event_type: 'electrification', date: '1935-12-15', date_precision: 'day', segment_id: 'SEG_0001', description: 'Moscow-Petersburg electrified' },
          { event_id: 'EVT_SEG_0009', event_type: 'segment_close', date: '1975-06-01', date_precision: 'month', segment_id: 'SEG_0005', description: 'Irkutsk segment closed' },
        ];

        const demoSegments: Segment[] = [
          { segment_id: 'SEG_0001', from_station_id: 'RU.STN.Moskva-Passazhirskaya-Paveletskaya', to_station_id: 'RU.STN.Sankt-Peterburg-Baltiyskiy', geometry: [[55.7765, 37.6550], [59.9311, 30.3609]], geometry_quality: 'high' },
          { segment_id: 'SEG_0002', from_station_id: 'RU.STN.Sankt-Peterburg-Baltiyskiy', to_station_id: 'RU.STN.Vladivostok', geometry: [[59.9311, 30.3609], [43.1056, 131.8735]], geometry_quality: 'high' },
          { segment_id: 'SEG_0003', from_station_id: 'RU.STN.Vladivostok', to_station_id: 'RU.STN.Ekaterinburg-Passazhirskiy', geometry: [[43.1056, 131.8735], [56.8519, 60.6122]], geometry_quality: 'medium' },
          { segment_id: 'SEG_0004', from_station_id: 'RU.STN.Ekaterinburg-Passazhirskiy', to_station_id: 'RU.STN.Novosibirsk-Glavnyy', geometry: [[56.8519, 60.6122], [55.0415, 82.9346]], geometry_quality: 'high' },
          { segment_id: 'SEG_0005', from_station_id: 'RU.STN.Novosibirsk-Glavnyy', to_station_id: 'RU.STN.Irkutsk-Passazhirskiy', geometry: [[55.0415, 82.9346], [52.2869, 104.3050]], geometry_quality: 'medium' },
          { segment_id: 'SEG_0006', from_station_id: 'RU.STN.Irkutsk-Passazhirskiy', to_station_id: 'STN_0007', geometry: [[52.2869, 104.3050], [60.0, 100.0]], geometry_quality: 'low' },
          { segment_id: 'SEG_0007', from_station_id: 'STN_0007', to_station_id: 'STN_0008', geometry: [[60.0, 100.0], [55.7887, 49.1221]], geometry_quality: 'high' },
        ];

        setStations(loadedStations);
        setStationNames(loadedStationNames);
        setEvents(demoEvents); // Keep mock events until real event data provided
        setSegments(demoSegments); // Keep mock segments until real segment data provided
      } catch (err: any) {
        setError(err?.message || 'Failed to load CSV data');
      } finally {
        setIsLoading(false);
      }
    };

    loadData();
  }, []);

  const queryDataForYear = (year: number): { stations: StationWithState[]; segments: SegmentWithState[] } => {
    const stationEventMap = new Map<string, Event[]>();
    events.forEach(event => {
      if (event.station_id) {
        if (!stationEventMap.has(event.station_id)) {
          stationEventMap.set(event.station_id, []);
        }
        stationEventMap.get(event.station_id)!.push(event);
      }
    });

    const namesByStation = new Map<string, Array<{ name: string; language: string }>>();
    stationNames.forEach(sn => {
      if (!namesByStation.has(sn.station_id)) {
        namesByStation.set(sn.station_id, []);
      }
      namesByStation.get(sn.station_id)!.push({ name: sn.name, language: sn.language });
    });

    const resultStations: StationWithState[] = [];

    stations.forEach(station => {
      const stationEvents = stationEventMap.get(station.station_id) || [];

      const openEvent = stationEvents
        .filter(e => e.event_type === 'station_open')
        .map(e => ({ ...e, year: new Date(e.date).getFullYear() }))
        .sort((a, b) => b.year - a.year)[0];

      const closeEvent = stationEvents
        .filter(e => e.event_type === 'station_close')
        .map(e => ({ ...e, year: new Date(e.date).getFullYear() }))
        .sort((a, b) => b.year - a.year)[0];

      const electrificationEvent = stationEvents
        .filter(e => e.event_type === 'electrification')
        .map(e => ({ ...e, year: new Date(e.date).getFullYear() }))
        .filter(e => e.year <= year)
        .sort((a, b) => b.year - a.year)[0];

      const openYear = openEvent ? openEvent.year : station.created_at ? new Date(station.created_at).getFullYear() : Infinity;
      const closeYear = closeEvent ? closeEvent.year : null;

      // Station built status
      let state: 'planned' | 'existing' | 'new' | 'electrified' | 'gauge_change' | 'closed';
      if (openYear === Infinity || openYear > year) {
        state = 'planned';
      } else {
        if (closeYear && closeYear < year) return; // closed before this year
        if (closeYear && closeYear === year) {
          state = 'closed';
        } else if (station.current_status === 'closed') {
          state = 'closed';
        } else if (electrificationEvent && electrificationEvent.year === year) {
          state = 'electrified';
        } else if (openEvent && openEvent.year === year) {
          state = 'new';
        } else {
          state = 'existing';
        }
      }

      const altNames: { [key: string]: string } = {};
      const stationNamesList = namesByStation.get(station.station_id) || [];
      const langCounts: { [key: string]: number } = {};

      for (const { name, language } of stationNamesList) {
        if (!langCounts[language]) {
          langCounts[language] = 0;
        }
        langCounts[language]++;
        const suffix = langCounts[language] > 1 ? `_${langCounts[language] - 1}` : '';
        altNames[`name:${language}${suffix}`] = name;
      }

      resultStations.push({
        ...station,
        state,
        alternative_names: altNames,
      });
    });

    const segmentEventMap = new Map<string, Event[]>();
    events.forEach(event => {
      if (event.segment_id) {
        if (!segmentEventMap.has(event.segment_id)) {
          segmentEventMap.set(event.segment_id, []);
        }
        segmentEventMap.get(event.segment_id)!.push(event);
      }
    });

    const resultSegments: SegmentWithState[] = [];

    segments.forEach(segment => {
      const segmentEvents = segmentEventMap.get(segment.segment_id) || [];

      const openEvent = segmentEvents
        .filter(e => e.event_type === 'segment_open')
        .map(e => ({ ...e, year: new Date(e.date).getFullYear() }))
        .filter(e => e.year <= year)
        .sort((a, b) => b.year - a.year)[0];

      const closeEvent = segmentEvents
        .filter(e => e.event_type === 'segment_close')
        .map(e => ({ ...e, year: new Date(e.date).getFullYear() }))
        .filter(e => e.year <= year)
        .sort((a, b) => b.year - a.year)[0];

      const electrificationEvent = segmentEvents
        .filter(e => e.event_type === 'electrification')
        .map(e => ({ ...e, year: new Date(e.date).getFullYear() }))
        .filter(e => e.year <= year)
        .sort((a, b) => b.year - a.year)[0];

      const openYear = openEvent ? openEvent.year : -Infinity;
      const closeYear = closeEvent ? closeEvent.year : null;

      if (year < openYear) return;
      if (closeYear && closeYear < year) return;

      let state: 'existing' | 'new' | 'electrified' | 'gauge_change' | 'closed';
      if (closeYear && closeYear === year) {
        state = 'closed';
      } else if (electrificationEvent && electrificationEvent.year === year) {
        state = 'electrified';
      } else if (openEvent && openEvent.year === year) {
        state = 'new';
      } else {
        state = 'existing';
      }

      resultSegments.push({
        ...segment,
        state,
      });
    });

    return { stations: resultStations, segments: resultSegments };
  };

  return (
    <DatabaseContext.Provider value={{ stations, stationNames, events, segments, isLoading, error, queryDataForYear }}>
      {children}
    </DatabaseContext.Provider>
  );
}
