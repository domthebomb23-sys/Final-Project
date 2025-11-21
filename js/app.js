// Frontend app: loads data/disasters_geo.json and shows markers on a Leaflet map.

const DATA_URL = 'data/disasters_geo.json';

const map = L.map('map', {zoomControl:true}).setView([39.5, -98.35], 4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '© OpenStreetMap contributors'
}).addTo(map);

// Use marker clustering for a nicer display when markers overlap
const markerCluster = L.markerClusterGroup();
map.addLayer(markerCluster);

let allFeatures = [];
const status = document.getElementById('status');
const filterSelect = document.getElementById('filter');
const searchInput = document.getElementById('search');
const yearRange = document.getElementById('yearRange');
const yearValue = document.getElementById('yearValue');

function updateStatus(s){ status.textContent = s }

function populateFilter(types){
  const uniq = Array.from(new Set(types)).filter(Boolean).sort();
  uniq.forEach(t => {
    const opt = document.createElement('option');
    opt.value = t;
    opt.textContent = t;
    filterSelect.appendChild(opt);
  });
}

function makePopup(props){
  const d = [];
  if(props.year) d.push('<b>Year:</b> ' + props.year);
  if(props.disaster) d.push('<b>Type:</b> ' + props.disaster);
  if(props.article) d.push('<b>Article:</b> ' + (props.article || ''));
  if(props.location) d.push('<b>Location:</b> ' + props.location);
  return d.join('<br/>');
}

function addMarkers(features){
  markerCluster.clearLayers();
  features.forEach(f => {
    const [lon, lat] = f.geometry.coordinates;
    const marker = L.marker([lat, lon]);
    marker.bindPopup(makePopup(f.properties));
    markerCluster.addLayer(marker);
  });
  if(features.length){
    const latlngs = features.map(f => [f.geometry.coordinates[1], f.geometry.coordinates[0]]);
    const bounds = L.latLngBounds(latlngs);
    map.fitBounds(bounds.pad(0.2));
  }
}

function applyFilters(){
  const typeFilter = filterSelect.value.trim().toLowerCase();
  const search = searchInput.value.trim().toLowerCase();
  const yearMax = parseInt(yearRange.value, 10) || 9999;

  const filtered = allFeatures.filter(f => {
    const props = f.properties || {};
    if(typeFilter && !(props.disaster || '').toLowerCase().includes(typeFilter)) return false;
    if(search){
      const hay = ((props.disaster||'') + ' ' + (props.location||'') + ' ' + (props.article||'')).toLowerCase();
      if(!hay.includes(search)) return false;
    }
    if(props.year){
      const y = parseInt(String(props.year).split(' ')[0].split('/')[0], 10);
      if(!isNaN(y) && y > yearMax) return false;
    }
    return true;
  });

  addMarkers(filtered);
  updateStatus(`Showing ${filtered.length} events${typeFilter ? ' filtered by "'+typeFilter+'"' : ''}`);
}

fetch(DATA_URL).then(r => r.json()).then(data => {
  allFeatures = data.features || [];
  populateFilter(allFeatures.map(f => f.properties.disaster || ''));
  addMarkers(allFeatures);
  updateStatus(`Loaded ${allFeatures.length} events.`);
}).catch(err => {
  console.error(err);
  updateStatus('Failed to load data: ' + err.message);
});

filterSelect.addEventListener('change', applyFilters);
searchInput.addEventListener('input', () => { setTimeout(applyFilters, 200); });
yearRange.addEventListener('input', () => { yearValue.textContent = yearRange.value; applyFilters(); });

