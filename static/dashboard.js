const PALETTES = {
  medallion: ['#B8722F', '#AEB7C0', '#D9A62E', '#8B9096', '#6E4A22', '#7C858C'],
  ocean:     ['#2C6E8E', '#4FA3C4', '#8FD0E8', '#1B4A5E', '#0E2A36', '#6FBBD6'],
  sunset:    ['#D9642E', '#E89A4D', '#F2C572', '#B8442E', '#7A2A1E', '#EDB06B'],
  mono:      ['#EDEDEA', '#B8B8B0', '#8B9096', '#5A5E62', '#33373C', '#D0D0C8'],
};

function applyTheme(name) {
  document.documentElement.setAttribute('data-theme', name);
  document.querySelectorAll('[data-theme-btn]').forEach(btn => {
    btn.classList.toggle('active', btn.getAttribute('data-theme-btn') === name);
  });
  try { localStorage.setItem('analystWorldTheme', name); } catch (e) {}
}

function initTheme() {
  let saved = 'dark';
  try { saved = localStorage.getItem('analystWorldTheme') || 'dark'; } catch (e) {}
  applyTheme(saved);
}

let _chartInstance = null;

function renderChart(canvasId, chartType, paletteName, labels, values, valueLabel) {
  const palette = PALETTES[paletteName] || PALETTES.medallion;
  const ctx = document.getElementById(canvasId).getContext('2d');
  if (_chartInstance) { _chartInstance.destroy(); }

  const isMultiColor = (chartType === 'pie' || chartType === 'doughnut' || chartType === 'bar');
  const colors = isMultiColor
    ? labels.map((_, i) => palette[i % palette.length])
    : palette[0];

  const config = {
    type: chartType,
    data: {
      labels: labels,
      datasets: [{
        label: valueLabel,
        data: values,
        backgroundColor: colors,
        borderColor: chartType === 'line' ? palette[0] : (isMultiColor ? colors : palette[0]),
        borderWidth: chartType === 'line' ? 2 : 1,
        pointBackgroundColor: palette[2] || palette[0],
        tension: 0.25,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: (chartType === 'pie' || chartType === 'doughnut') } },
      scales: (chartType === 'pie' || chartType === 'doughnut') ? {} : {
        x: { ticks: { color: '#8B9096' }, grid: { color: 'rgba(255,255,255,0.05)' } },
        y: { ticks: { color: '#8B9096' }, grid: { color: 'rgba(255,255,255,0.05)' }, beginAtZero: true }
      }
    }
  };

  _chartInstance = new Chart(ctx, config);
  return palette;
}

function renderLegend(elId, palette, labels) {
  const el = document.getElementById(elId);
  if (!el) return;
  el.innerHTML = labels.map((l, i) =>
    `<span><span class="sw" style="background:${palette[i % palette.length]}"></span>${l}</span>`
  ).join('');
}

function toNumericSeries(rows, xCol, yCol) {
  const labels = [];
  const values = [];
  rows.forEach(row => {
    const y = parseFloat(row[yCol]);
    if (!isNaN(y)) {
      labels.push(String(row[xCol] ?? ''));
      values.push(y);
    }
  });
  return { labels, values };
}
