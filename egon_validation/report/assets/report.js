
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

  // Applied Rule Types: unique rule_classes that were applied
  const appliedRuleTypes = coverage.coverage_statistics?.rule_coverage?.applied_rules ?? new Set(items.map(x => x.rule_class)).size;
  $("#kpi-rule-classes-applied").textContent = String(appliedRuleTypes);

  // Rules in Scope: total unique rules available in registry (for coverage calculation)
  const totalUniqueRulesInRegistry = coverage.coverage_statistics?.rule_coverage?.total_rules ?? appliedRuleTypes;
  $("#kpi-rule-classes-total").textContent = String(totalUniqueRulesInRegistry);

  // Total Rules: all registered rule instances (sum of passed + failed)
  const totalRuleInstances = coverage.coverage_statistics?.validation_results?.total_applications ?? total;
  $("#kpi-total-rule-instances").textContent = String(totalRuleInstances);

  $("#kpi-passed").textContent = String(passed);
  $("#kpi-failed").textContent = String(failed);

  const tablesTotal = coverage.tables_total ?? (new Set(items.map(x => x.dataset))).size;
  const tablesValidated = coverage.tables_validated ?? (new Set(items.map(x => x.dataset))).size;
  $("#kpi-tables-validated").textContent = String(tablesValidated);
  $("#kpi-tables-total").textContent = String(tablesTotal);

  // Display coverage statistics
  const coverageStats = coverage.coverage_statistics;
  if (coverageStats) {
    // Table coverage
    const tableCoverage = coverageStats.table_coverage;
    $("#kpi-table-coverage").textContent = `${tableCoverage.percentage}%`;
    const tableCoverageDetails = $("#coverage-table-details");
    if (tableCoverageDetails) tableCoverageDetails.textContent = `${tableCoverage.validated_tables} / ${tableCoverage.total_tables} tables`;

    // Rule coverage
    const ruleCoverage = coverageStats.rule_coverage;
    $("#kpi-rule-coverage").textContent = `${ruleCoverage.percentage}%`;
    const ruleCoverageDetails = $("#coverage-rule-details");
    if (ruleCoverageDetails) ruleCoverageDetails.textContent = `${ruleCoverage.applied_rules} / ${ruleCoverage.total_rules} rules`;

    // Success rate
    const validationResults = coverageStats.validation_results;
    $("#kpi-success-rate").textContent = `${validationResults.success_rate}%`;
    const successDetails = $("#coverage-success-details");
    if (successDetails) successDetails.textContent = `${validationResults.successful} / ${validationResults.total_applications} validations`;

    // Rule application statistics
    if (coverageStats.rule_application_stats && coverageStats.rule_application_stats.length > 0) {
      const ruleStatsSection = document.getElementById('rule-application-stats');
      const ruleStatsTable = document.getElementById('rule-stats-table');
      
      const table = document.createElement('table');
      table.innerHTML = `
        <thead>
          <tr>
            <th>Rule</th>
            <th>Applications</th>
          </tr>
        </thead>
        <tbody></tbody>
      `;

      const tbody = table.querySelector('tbody');
      // Sort by applications descending
      const sortedStats = [...coverageStats.rule_application_stats].sort((a, b) => b.applications - a.applications);
      sortedStats.forEach(stat => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${stat.rule_class}</td>
          <td><span class="application-count">${stat.applications}</span></td>
        `;
        tbody.appendChild(tr);
      });
      
      ruleStatsTable.appendChild(table);
      ruleStatsSection.style.display = 'block';

      // Add toggle functionality for rule stats section
      const h3 = ruleStatsSection.querySelector('h3');
      const tableContainer = ruleStatsTable;
      const baseHeading = h3.textContent.trim(); // Use heading from HTML
      let isExpanded = false;

      h3.addEventListener('click', function() {
        isExpanded = !isExpanded;
        tableContainer.style.display = isExpanded ? 'block' : 'none';
        h3.textContent = `${baseHeading} ${isExpanded ? '▼' : '▶'}`;
      });

      h3.textContent = `${baseHeading} ▶`;
      tableContainer.style.display = 'none';
    }
  }

  // Matrix
  const datasets = coverage.datasets && coverage.datasets.length ? coverage.datasets : [...new Set(items.map(x => x.dataset))];
  const rules = coverage.rules_formal && coverage.rules_formal.length ? coverage.rules_formal : [...new Set(items.map(x => x.rule_class))];

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

  // Create a simple lookup: (table, rule_class) -> rule_id from results.json
  // This is needed because matrix uses rule_class but details use rule_id
  const tableRuleClassToRuleId = new Map();
  for (const item of items) {
    const key = `${item.table}::${item.rule_class || item.rule_id}`;
    // Store first matching rule_id for this combination
    if (!tableRuleClassToRuleId.has(key)) {
      tableRuleClassToRuleId.set(key, item.rule_id);
    }
  }

  const mwrap = document.createElement('div'); mwrap.className = 'matrix-wrapper';
  const tbl = document.createElement('table'); tbl.className = 'matrix';
  
  // Set CSS variable for actual column count
  document.documentElement.style.setProperty('--column-count', rules.length);

  // Function to create meaningful abbreviations from rule class names
  function createAbbreviation(ruleName) {
    const abbreviations = {
      // Rule class names (CamelCase)
      'ArrayCardinalityValidation': 'ArrayCard',
      'DataTypeValidation': 'DataType',
      'MultipleColumnsDataTypeValidation': 'MultiColType',
      'GeometryContainmentValidation': 'GeomContain',
      'NotNullAndNotNaN': 'NotNull',
      'NullCheckValidation': 'NullCheck',
      'ReferentialIntegrityValidation': 'RefIntegrity',
      'RowCountValidation': 'RowCount',
      'RowCountComparisonValidation': 'RowCountComp',
      'SRIDValidation': 'SRID',
      'SRIDUniqueNonZero': 'SRID-Uniq',
      'ValueSetValidation': 'ValueSet',
      'DisaggregatedDemandSumValidation': 'DemandSum',
      'ElectricalLoadAggregationValidation': 'LoadAggr',
    };

    // Custom abbreviation exists
    if (abbreviations[ruleName]) {
      return abbreviations[ruleName];
    }

    // Auto-generate from CamelCase: extract capital letters and some context
    // E.g., "SomeValidationRule" -> "SomeValid"
    if (/^[A-Z][a-z]/.test(ruleName)) {
      // CamelCase detected
      const parts = ruleName.match(/[A-Z][a-z]+/g) || [];
      if (parts.length >= 2) {
        // Take first word + part of second word
        return parts[0] + parts[1].substring(0, Math.min(4, parts[1].length));
      }
      return ruleName.substring(0, 10);
    }

    // Fallback for SNAKE_CASE: take first letter of each word
    const words = ruleName.split('_');
    if (words.length > 1) {
      let abbr = words.map(w => w[0]).join('').substring(0, 6);
      return abbr;
    }

    return ruleName.substring(0, 10);
  }

  const thead = document.createElement('thead'); const hr = document.createElement('tr');
  hr.innerHTML = '<th>Schema.Table \\\\ Rule</th>' +
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
      // Look up the actual rule_id for this (table, rule_class) combination
      const ruleIdForNav = tableRuleClassToRuleId.get(`${d}::${r}`) || r;
      const dataAttrs = status !== 'na' ? `data-dataset="${d}" data-rule="${ruleIdForNav}"` : '';
      const icon = status==='ok'   ? `<span class="icon ok ${clickable}" title="${title}" ${dataAttrs}>✅</span>` :
                    status==='fail' ? `<span class="icon fail ${clickable}" title="${title}" ${dataAttrs}>❌</span>` :
                                      `<span class="icon na" title="${title}">●</span>`;
      rowHtml += `<td>${icon}</td>`;
    }
    const customs = (coverage.custom_checks && coverage.custom_checks[d]) ? coverage.custom_checks[d] : [];
    const customHtml = customs.length ? customs.map(name => `<span class="tag has-tooltip clickable" title="Custom check: ${name}" data-dataset="${d}" data-rule="${name}">${name}</span>`).join('') : '<span class="badge empty">—</span>';
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
        <th>Status</th><th>Observed</th><th>Expected</th><th>Executed At</th><th>Message</th>
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
    const executedAt = r.executed_at ? new Date(r.executed_at).toLocaleString() : '';

    // Format observed value: show "<0.01" for very small positive numbers
    let observedDisplay = r.observed ?? '';
    if (observedDisplay !== '' && !isNaN(observedDisplay)) {
      const obsNum = parseFloat(observedDisplay);
      if (obsNum > 0 && obsNum < 0.01) {
        observedDisplay = '<0.01';
      }
    }

    // Extract schema and table name from table field if needed
    let schema = r.schema;
    let tableName = r.table;
    if (r.table && r.table.includes('.')) {
      const parts = r.table.split('.');
      if (!schema) {
        schema = parts[0];
      }
      tableName = parts[1];
    }

    tr.id = `detail-${r.table}-${r.rule_id}`;
    tr.innerHTML = `
      <td class="has-tooltip" title="${r.task ?? ''}">${r.task ?? ''}</td>
      <td class="has-tooltip" title="${schema ?? ''}">${schema ?? ''}</td>
      <td class="has-tooltip" title="${r.table ?? ''}">${tableName ?? ''}</td>
      <td class="has-tooltip" title="${r.column ?? ''}">${r.column ?? ''}</td>
      <td class="has-tooltip" title="${r.rule_id}">${r.rule_id}</td>
      <td class="has-tooltip" title="${r.severity ?? ''}">${r.severity}</td>
      <td class="has-tooltip" title="Validation ${r.success ? 'passed' : 'failed'}">${badge}</td>
      <td class="has-tooltip" title="Observed value: ${observedDisplay}">${observedDisplay}</td>
      <td class="has-tooltip" title="Expected value: ${r.expected ?? ''}">${r.expected ?? ''}</td>
      <td class="has-tooltip" title="${executedAt}">${executedAt}</td>
      <td class="has-tooltip" title="${r.message ?? ''}">${r.message ?? ''}</td>`;
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
