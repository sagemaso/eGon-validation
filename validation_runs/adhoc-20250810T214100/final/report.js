
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

  const thead = document.createElement('thead'); const hr = document.createElement('tr');
  hr.innerHTML = '<th>Dataset \\\\ Rule</th>' + rules.map(r => `<th>${r}</th>`).join('') + '<th>Custom checks</th>';
  thead.appendChild(hr); tbl.appendChild(thead);

  const tbody = document.createElement('tbody');
  for (const d of datasets) {
    const tr = document.createElement('tr');
    let rowHtml = `<td><strong>${d}</strong></td>`;
    for (const r of rules) {
      const { status, title } = cellMap.get(d+'::'+r);
      const icon = status==='ok'   ? `<span class="icon ok"   title="${title}">✅</span>` :
                    status==='fail' ? `<span class="icon fail" title="${title}">❌</span>` :
                                      `<span class="icon na"   title="${title}">🔵</span>`;
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
  for (const r of items) {
    const tr = document.createElement('tr');
    const status = r.success ? 'OK' : 'FAIL';
    const badge = `<span class="badge ${r.success ? 'ok' : 'fail'}">${status}</span>`;
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
})();
