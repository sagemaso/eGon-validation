
(async function () {
  const $ = (sel) => document.querySelector(sel);

  async function loadJson(url, fallback) {
    try { return await fetch(url).then(r => r.json()); }
    catch (e) { return fallback; }
  }
  const results = await loadJson("./results.json", { items: [] });
  const coverage = await loadJson("./coverage.json", { datasets: [], rules_formal: [], cells: [], custom_checks: {} });

  const items = results.items || [];
  const total = items.length;
  const passed = items.filter(x => x.success).length;
  const failed = total - passed;

  $("#kpi-total-rules").textContent = String(total);
  $("#kpi-passed").textContent = String(passed);
  $("#kpi-failed").textContent = String(failed);

  const tablesTotal = coverage.tables_total ?? (new Set(items.map(x => x.dataset))).size;
  const tablesValidated = coverage.tables_validated ?? (new Set(items.map(x => x.dataset))).size;
  $("#kpi-tables-validated").textContent = String(tablesValidated);
  $("#kpi-tables-total").textContent = String(tablesTotal);

  // Matrix
  const datasets = coverage.datasets && coverage.datasets.length ? coverage.datasets : [...new Set(items.map(x => x.dataset))];
  const rules = coverage.rules_formal && coverage.rules_formal.length ? coverage.rules_formal : [...new Set(items.map(x => x.rule_id))];

  // Build a map of (dataset, rule) -> {status, title}
  const cellMap = new Map();
  for (const c of (coverage.cells || [])) {
    cellMap.set(c.dataset + '::' + c.rule_id, { status: c.status, title: c.title || '' });
  }
  // Fill missing with 'na'
  for (const d of datasets) {
    for (const r of rules) {
      const key = d + '::' + r;
      if (!cellMap.has(key)) cellMap.set(key, { status: 'na', title: 'Not applied' });
    }
  }

  const mwrap = document.createElement('div'); mwrap.className = 'matrix-wrapper';
  const tbl = document.createElement('table'); tbl.className = 'matrix';
  
  // Set CSS variable for actual column count
  document.documentElement.style.setProperty('--column-count', rules.length);

  // Function to create meaningful abbreviations
  function createAbbreviation(ruleName) {
    const abbreviations = {
      'ARRAY_CARDINALITY_CHECK': 'ARC',
      'REFERENTIAL_INTEGRITY_CHECK': 'RIC', 
      'COLUMN_DATA_TYPE_CHECK': 'DTC',
      'MULTIPLE_COLUMNS_TYPE_CHECK': 'MCTC',
      'VALUE_SET_VALIDATION': 'VSV',
      'SRID_VALIDATION': 'SRID',
      'SRID_UNIQUE_NONZERO': 'SRID-UNZ',
      'SPECIAL_SRID_VALIDATION': 'SRID-SP',
      'SCENARIO_VALUES_VALID': 'SVV',
      'LOAD_TIMESERIES_LENGTH': 'TSL',
      'TS_LENGTH_CHECK': 'TSC',
      'WIND_PLANTS_IN_GERMANY': 'WPG',
      'MV_GRID_DISTRICT_COUNT': 'MGDC',
      'CTS_IND_ROW_COUNT_MATCH': 'CTS-RC',
      'DISAGGREGATED_DEMAND_SUM_MATCH': 'DDSM',
      'ELECTRICAL_LOAD_AGGREGATION': 'ELA',
      'LP_RANGE': 'LPR',
      'BAL_DIFF': 'BAL'
    };
    
    // Custom abbreviation exists
    if (abbreviations[ruleName]) {
      return abbreviations[ruleName];
    }
    
    // Auto-generate: take first letter of each word, max 4 chars
    const words = ruleName.split('_');
    let abbr = words.map(w => w[0]).join('').substring(0, 4);
    return abbr;
  }

  const thead = document.createElement('thead'); const hr = document.createElement('tr');
  hr.innerHTML = '<th>Dataset \\\\ Rule</th>' + 
    rules.map(r => `<th title="${r}">${createAbbreviation(r)}</th>`).join('') + 
    '<th>Custom checks</th>';
  thead.appendChild(hr); tbl.appendChild(thead);

  const tbody = document.createElement('tbody');
  for (const d of datasets) {
    const tr = document.createElement('tr');
    let rowHtml = `<td><strong>${d}</strong></td>`;
    for (const r of rules) {
      const { status, title } = cellMap.get(d+'::'+r);
      const clickable = status !== 'na' ? 'clickable' : '';
      const dataAttrs = status !== 'na' ? `data-dataset="${d}" data-rule="${r}"` : '';
      const icon = status==='ok'   ? `<span class="icon ok ${clickable}" title="${title}" ${dataAttrs}>✅</span>` :
                    status==='fail' ? `<span class="icon fail ${clickable}" title="${title}" ${dataAttrs}>❌</span>` :
                                      `<span class="icon na" title="${title}">●</span>`;
      rowHtml += `<td>${icon}</td>`;
    }
    const customs = (coverage.custom_checks && coverage.custom_checks[d]) ? coverage.custom_checks[d] : [];
    const customHtml = customs.length ? customs.map(name => `<span class="tag" title="custom">${name}</span>`).join('') : '<span class="badge empty">—</span>';
    rowHtml += `<td class="custom-cell">${customHtml}</td>`;
    tr.innerHTML = rowHtml; tbody.appendChild(tr);
  }
  tbl.appendChild(tbody); mwrap.appendChild(tbl);
  document.querySelector('#coverage-matrix').appendChild(mwrap);


  // Details
  const det = document.createElement('table');
  det.innerHTML = `
    <thead>
      <tr>
        <th>Task</th><th>Schema</th><th>Table</th><th>Column</th>
        <th>Rule</th><th>Severity</th>
        <th>Status</th><th>Observed</th><th>Expected</th><th>Message</th>
      </tr>
    </thead>
    <tbody></tbody>`;
  const detBody = det.querySelector('tbody');
  
  // Sort items by task, schema, table, column alphabetically
  const sortedItems = items.sort((a, b) => {
    const taskA = (a.task ?? '').toLowerCase();
    const taskB = (b.task ?? '').toLowerCase();
    if (taskA !== taskB) return taskA.localeCompare(taskB);
    
    const schemaA = (a.schema ?? '').toLowerCase();
    const schemaB = (b.schema ?? '').toLowerCase();
    if (schemaA !== schemaB) return schemaA.localeCompare(schemaB);
    
    const tableA = (a.table ?? '').toLowerCase();
    const tableB = (b.table ?? '').toLowerCase();
    if (tableA !== tableB) return tableA.localeCompare(tableB);
    
    const columnA = (a.column ?? '').toLowerCase();
    const columnB = (b.column ?? '').toLowerCase();
    return columnA.localeCompare(columnB);
  });
  
  for (const r of sortedItems) {
    const tr = document.createElement('tr');
    const status = r.success ? 'OK' : 'FAIL';
    const badge = `<span class="badge ${r.success ? 'ok' : 'fail'}">${status}</span>`;
    tr.id = `detail-${r.dataset}-${r.rule_id}`;
    tr.innerHTML = `
      <td>${r.task ?? ''}</td>
      <td>${r.schema ?? ''}</td>
      <td>${r.table ?? ''}</td>
      <td>${r.column ?? ''}</td>
      <td>${r.rule_id}</td>
      <td>${r.severity}</td>
      <td>${badge}</td>
      <td>${r.observed ?? ''}</td>
      <td>${r.expected ?? ''}</td>
      <td>${r.message ?? ''}</td>`;
    detBody.appendChild(tr);
  }
  document.querySelector('#results-table').appendChild(det);

  // Add click handlers for matrix icons
  document.addEventListener('click', function(e) {
    if (e.target.classList.contains('clickable')) {
      const dataset = e.target.getAttribute('data-dataset');
      const rule = e.target.getAttribute('data-rule');
      if (dataset && rule) {
        const targetRow = document.getElementById(`detail-${dataset}-${rule}`);
        if (targetRow) {
          targetRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
          // Highlight the row briefly
          targetRow.style.backgroundColor = '#2a2f38';
          setTimeout(() => {
            targetRow.style.backgroundColor = '';
          }, 2000);
        }
      }
    }
  });

  // JavaScript Sticky Header for Matrix
  function handleMatrixStickyHeader() {
    const matrixHeader = document.querySelector('.matrix thead');
    const matrixTable = document.querySelector('.matrix table');
    const detailsTable = document.querySelector('#results-table table');
    
    if (!matrixHeader || !matrixTable || !detailsTable) return;
    
    const matrixRect = matrixTable.getBoundingClientRect();
    const detailsRect = detailsTable.getBoundingClientRect();
    
    const isMatrixVisible = matrixRect.top <= 0 && matrixRect.bottom > 0;
    const isDetailsVisible = detailsRect.top <= 100;
    
    if (isMatrixVisible && !isDetailsVisible) {
      // Make header sticky
      matrixHeader.classList.add('js-sticky');
      matrixHeader.style.width = matrixTable.offsetWidth + 'px';
      matrixHeader.style.left = matrixRect.left + 'px';
    } else {
      // Remove sticky
      matrixHeader.classList.remove('js-sticky');
      matrixHeader.style.width = '';
      matrixHeader.style.left = '';
    }
    
    // Hide when details table is visible
    if (isDetailsVisible) {
      matrixHeader.style.display = 'none';
    } else {
      matrixHeader.style.display = '';
    }
  }

  // Listen to scroll events
  window.addEventListener('scroll', handleMatrixStickyHeader);
  window.addEventListener('resize', handleMatrixStickyHeader);
  handleMatrixStickyHeader(); // Initial check
})();
