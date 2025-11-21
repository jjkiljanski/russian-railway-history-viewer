import { useEffect, useRef, useState } from 'react';
import { useDatabase } from './DatabaseContext';

interface Station {
  station_id: string;
  name_primary: string;
  lat: number;
  lon: number;
  country_code?: string;
  wikidata_id?: string;
  wikipedia_ru?: string;
  parovoz_url?: string;
  railwayz_id?: string;
  current_status: string;
  notes?: string;
  osm_node_id?: string;
  osm_way_id?: string;
  osm_relation_id?: string;
  state: 'planned' | 'existing' | 'new' | 'electrified' | 'gauge_change' | 'closed';
  alternative_names: { [key: string]: string };
}

interface Segment {
  segment_id: string;
  from_station_id: string;
  to_station_id: string;
  geometry: [number, number][];
  state: 'existing' | 'new' | 'electrified' | 'gauge_change' | 'closed';
}

interface MapViewProps {
  currentYear: number;
}

export function MapView({ currentYear }: MapViewProps) {
  const stationZoomThreshold = 6; // hide station markers until this zoom to reduce clutter
  const { queryDataForYear, isLoading } = useDatabase();
  const [stations, setStations] = useState<Station[]>([]);
  const [segments, setSegments] = useState<Segment[]>([]);
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<any>(null);
  const layersRef = useRef<any[]>([]);
  const drawLayerRef = useRef<any>(null);
  const drawControlRef = useRef<any>(null);
  const clearControlRef = useRef<any>(null);
  const [currentZoom, setCurrentZoom] = useState(4);

  useEffect(() => {
    if (!isLoading) {
      const data = queryDataForYear(currentYear);
      setStations(data.stations);
      setSegments(data.segments);
    }
  }, [currentYear, isLoading, queryDataForYear]);

  // Initialize map
  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return;

    // Wait for Leaflet to load
    const initMap = () => {
      if (typeof window === 'undefined' || !(window as any).L) {
        setTimeout(initMap, 100);
        return;
      }

      const L = (window as any).L;
      
      const map = L.map(mapRef.current).setView([55.7558, 37.6173], 4);
      
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      }).addTo(map);

      // Track zoom level
      map.on('zoomend', () => {
        setCurrentZoom(map.getZoom());
      });

      const initDrawControls = () => {
        if (!(window as any).L || !(window as any).L.Draw) {
          setTimeout(initDrawControls, 100);
          return;
        }

        if (drawLayerRef.current) return;

        const drawLayer = L.featureGroup().addTo(map);
        drawLayerRef.current = drawLayer;

        const drawControl = new L.Control.Draw({
          draw: {
            circle: {
              shapeOptions: {
                color: '#eab308',
                fillColor: '#fef08a',
                fillOpacity: 0.35,
                weight: 2,
              },
            },
            polyline: false,
            polygon: false,
            rectangle: false,
            marker: false,
            circlemarker: false,
          },
          edit: {
            featureGroup: drawLayer,
            remove: true,
          },
        });

        map.addControl(drawControl);
        drawControlRef.current = drawControl;

        const CopyToast = (message: string) => {
          if (navigator?.clipboard?.writeText) {
            navigator.clipboard.writeText(message);
          } else {
            window.prompt('Copy to clipboard:', message);
          }
        };

        const handleDrawCreated = (event: any) => {
          const layer = event.layer;
          drawLayer.addLayer(layer);

          const latlng = layer.getLatLng ? layer.getLatLng() : { lat: 0, lng: 0 };
          const radiusMeters = layer.getRadius ? layer.getRadius() : 0;
          const radiusKm = radiusMeters / 1000;

          const payload = `""",,,${latlng.lat.toFixed(4)},${latlng.lng.toFixed(4)},,,,,,,,,,,,,<radius ${radiusKm.toFixed(2)}>"""`;
          CopyToast(payload);

          if (layer.bindPopup) {
            layer.bindPopup(`<div><strong>Mock station</strong><br/>${payload}</div>`);
          }
        };

        map.on(L.Draw.Event.CREATED, handleDrawCreated);

        // Clear-control button
        const ClearControl = L.Control.extend({
          options: { position: 'topleft' },
          onAdd: () => {
            const container = L.DomUtil.create('div', 'leaflet-bar');
            const button = L.DomUtil.create('button', '', container);
            button.type = 'button';
            button.title = 'Clear drawn circles';
            button.innerText = 'Clear';
            button.style.width = '60px';
            button.style.height = '30px';
            button.style.background = '#fff';
            button.style.cursor = 'pointer';

            L.DomEvent.disableClickPropagation(button);
            L.DomEvent.on(button, 'click', () => {
              drawLayer.clearLayers();
            });

            return container;
          },
        });

        const clearControl = new ClearControl();
        clearControl.addTo(map);
        clearControlRef.current = clearControl;

        // Cleanup listeners when unmounting
        map.on('unload', () => {
          map.off(L.Draw.Event.CREATED, handleDrawCreated);
        });
      };

      initDrawControls();

      mapInstanceRef.current = map;
    };

    initMap();

    return () => {
      if (mapInstanceRef.current) {
        if (drawLayerRef.current) {
          drawLayerRef.current.clearLayers();
          drawLayerRef.current = null;
        }
        if (drawControlRef.current && mapInstanceRef.current.removeControl) {
          mapInstanceRef.current.removeControl(drawControlRef.current);
          drawControlRef.current = null;
        }
        if (clearControlRef.current && mapInstanceRef.current.removeControl) {
          mapInstanceRef.current.removeControl(clearControlRef.current);
          clearControlRef.current = null;
        }
        mapInstanceRef.current.remove();
        mapInstanceRef.current = null;
      }
    };
  }, []);

  // Update layers when data changes
  useEffect(() => {
    if (!mapInstanceRef.current || !(window as any).L) return;

    const L = (window as any).L;

    // Clear existing layers
    layersRef.current.forEach(layer => layer.remove());
    layersRef.current = [];

    // Render segments first (so they appear below stations)
    segments.forEach(segment => {
      // Determine color based on state
      let color = '#000000'; // existing - black
      let weight = 3;
      
      if (segment.state === 'new') {
        color = '#16a34a'; // green
        weight = 4;
      } else if (segment.state === 'electrified') {
        color = '#ea580c'; // orange
        weight = 4;
      } else if (segment.state === 'gauge_change') {
        color = '#9333ea'; // purple
        weight = 4;
      } else if (segment.state === 'closed') {
        color = '#dc2626'; // red
        weight = 4;
      }

      const polyline = L.polyline(segment.geometry, {
        color: color,
        weight: weight,
        opacity: 0.7,
      });

      polyline.addTo(mapInstanceRef.current);
      layersRef.current.push(polyline);
    });

    // Compute which stations are endpoints of visible segments
    const endpointStations = new Set<string>();
    segments.forEach(segment => {
      endpointStations.add(segment.from_station_id);
      endpointStations.add(segment.to_station_id);
    });

    // Then render stations
    stations.forEach(station => {
      const isMock = station.current_status === 'mock';
      const isEndpoint = endpointStations.has(station.station_id);
      const isPlanned = station.state === 'planned';

      // Skip non-endpoint stations when zoomed out
      if (!isEndpoint && currentZoom < stationZoomThreshold) {
        return;
      }
      
      // Build popup content
      let popupHTML = `
        <div style="min-width: 200px;">
          <h3 style="margin-bottom: 0.5rem; font-weight: bold;">${station.name_primary}</h3>
          <div style="font-size: 0.875rem;">
            <div><strong>ID:</strong> ${station.station_id}</div>
            <div><strong>Location:</strong> ${station.lat.toFixed(4)}, ${station.lon.toFixed(4)}</div>
      `;

      if (station.country_code) {
        popupHTML += `<div><strong>Country:</strong> ${station.country_code}</div>`;
      }
      
      if (station.current_status) {
        popupHTML += `<div><strong>Status:</strong> ${station.current_status}</div>`;
      }

      if (station.osm_node_id) {
        popupHTML += `<div><strong>OSM Node:</strong> <a href="https://www.openstreetmap.org/node/${station.osm_node_id}" target="_blank" rel="noopener noreferrer" style="color: #2563eb; text-decoration: underline;">${station.osm_node_id}</a></div>`;
      }

      if (station.wikidata_id) {
        popupHTML += `<div><strong>Wikidata:</strong> <a href="https://www.wikidata.org/wiki/${station.wikidata_id}" target="_blank" rel="noopener noreferrer" style="color: #2563eb; text-decoration: underline;">${station.wikidata_id}</a></div>`;
      }

      if (station.wikipedia_ru) {
        popupHTML += `<div><strong>Wikipedia (RU):</strong> <a href="https://ru.wikipedia.org/wiki/${encodeURIComponent(station.wikipedia_ru)}" target="_blank" rel="noopener noreferrer" style="color: #2563eb; text-decoration: underline;">${station.wikipedia_ru}</a></div>`;
      }

      if (station.parovoz_url) {
        popupHTML += `<div><strong>Parovoz:</strong> <a href="${station.parovoz_url}" target="_blank" rel="noopener noreferrer" style="color: #2563eb; text-decoration: underline;">Link</a></div>`;
      }

      Object.entries(station.alternative_names).forEach(([key, value]) => {
        popupHTML += `<div><strong>${key}:</strong> ${value}</div>`;
      });

      if (station.notes) {
        popupHTML += `<div style="margin-top: 0.5rem; padding-top: 0.5rem; border-top: 1px solid #e2e8f0;"><strong>Notes:</strong> ${station.notes}</div>`;
      }

      popupHTML += `</div></div>`;

      if (isMock) {
        // Extract radius if available
        let radius = 5; // default radius in km
        if (station.notes) {
          const match = station.notes.match(/<radius\s+([\d.]+)>/);
          if (match) {
            radius = parseFloat(match[1]);
          }
        }
        
        // Show circle only at zoom level 7 or higher
        if (currentZoom >= 7) {
          const radiusMeters = radius * 1000;
          
          const circle = L.circle([station.lat, station.lon], {
            radius: radiusMeters,
            color: '#eab308', // yellow
            fillColor: '#eab308',
            fillOpacity: 0.15,
            weight: 2,
            opacity: 0.6,
          });

          circle.bindPopup(popupHTML);
          circle.addTo(mapInstanceRef.current);
          layersRef.current.push(circle);
        }
        
        // Always show a regular marker for mock stations
        const marker = L.circleMarker([station.lat, station.lon], {
          radius: 5,
          color: '#eab308', // yellow
          fillColor: '#eab308',
          fillOpacity: 0.8,
          weight: 2,
        });

        marker.bindPopup(popupHTML);
        marker.addTo(mapInstanceRef.current);
        layersRef.current.push(marker);
      } else {
        // Regular station - planned stations in grey, built stations in black
        const markerColor = isPlanned ? '#94a3b8' : '#000000';
        const marker = L.circleMarker([station.lat, station.lon], {
          radius: 4,
          color: markerColor,
          fillColor: markerColor,
          fillOpacity: isPlanned ? 0.6 : 0.9,
          weight: 1,
        });

        marker.bindPopup(popupHTML);
        marker.addTo(mapInstanceRef.current);
        layersRef.current.push(marker);
      }
    });
  }, [stations, segments, currentZoom]);

  if (isLoading) {
    return (
      <div className="h-full flex items-center justify-center bg-slate-200">
        <p>Loading map data...</p>
      </div>
    );
  }

  return (
    <div ref={mapRef} style={{ height: '100%', width: '100%' }} />
  );
}
