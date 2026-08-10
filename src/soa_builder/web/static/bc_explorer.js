// BC Grouping Explorer — adapted from cdisc-biomedical-concept-groupings/
// bc-explorer-prototype.html. Data is seeded server-side into BC_DATA
// (see templates/bc_explorer.html) instead of being loaded from an
// embedded mock or a client-side XLSX import.

let store = BC_DATA;
let activeFilters = {}; // scheme_id -> Set of value_ids
let collapsedSchemes = new Set();
let showZeros = false;
let searchQuery = '';
let searchAllProperties = false;
let currentView = 'explorer';
let selectedBcId = null;

// ─── Lookup helpers ───
function valueById(id) { return store.values.find(v => v.value_id === id); }
function schemeById(id) { return store.schemes.find(s => s.scheme_id === id); }
function bcById(id) { return store.bcs.find(b => b.bc_id === id); }
function assignmentsForBc(bcId) { return store.assignments.filter(a => a.bc_id === bcId); }
function bcsForValue(valueId) {
  const bcIds = new Set(store.assignments.filter(a => a.value_id === valueId).map(a => a.bc_id));
  return store.bcs.filter(b => bcIds.has(b.bc_id));
}
function valuesForScheme(schemeId) { return store.values.filter(v => v.scheme_id === schemeId); }

// ─── Search match helper ───
function bcMatchesSearch(bc, q) {
  if (bc.short_name.toLowerCase().includes(q)) return true;
  if (bc.bc_id.toLowerCase().includes(q)) return true;
  if (bc.ncit_code.toLowerCase().includes(q)) return true;
  if (!searchAllProperties) return false;
  const asgns = assignmentsForBc(bc.bc_id);
  return asgns.some(a => {
    const v = valueById(a.value_id);
    if (!v) return false;
    if (v.label.toLowerCase().includes(q)) return true;
    if (v.description && v.description.toLowerCase().includes(q)) return true;
    return false;
  });
}

// ─── Filtering logic ───
function getFilteredBcs() {
  let bcs = store.bcs;

  if (searchQuery) {
    const q = searchQuery.toLowerCase();
    bcs = bcs.filter(bc => bcMatchesSearch(bc, q));
  }

  // Facet filters: AND across schemes, OR within a scheme
  const activeSchemes = Object.keys(activeFilters).filter(k => activeFilters[k].size > 0);
  if (activeSchemes.length > 0) {
    bcs = bcs.filter(bc => {
      const asgns = assignmentsForBc(bc.bc_id);
      return activeSchemes.every(schemeId => {
        const selectedValues = activeFilters[schemeId];
        return asgns.some(a => a.scheme_id === schemeId && selectedValues.has(a.value_id));
      });
    });
  }

  return bcs;
}

function getValueCountForFiltered(valueId, schemeId) {
  // count how many currently-filtered BCs have this value
  // but ignore filters from the same scheme (so counts stay useful)
  const activeSchemes = Object.keys(activeFilters).filter(k => activeFilters[k].size > 0 && k !== schemeId);
  let bcs = store.bcs;

  if (searchQuery) {
    const q = searchQuery.toLowerCase();
    bcs = bcs.filter(bc => bcMatchesSearch(bc, q));
  }

  if (activeSchemes.length > 0) {
    bcs = bcs.filter(bc => {
      const asgns = assignmentsForBc(bc.bc_id);
      return activeSchemes.every(sid => {
        const sv = activeFilters[sid];
        return asgns.some(a => a.scheme_id === sid && sv.has(a.value_id));
      });
    });
  }

  const bcIds = new Set(bcs.map(b => b.bc_id));
  return store.assignments.filter(a => a.value_id === valueId && bcIds.has(a.bc_id)).length;
}

// ─── Highlight helper ───
function highlight(text, query) {
  if (!query) return text;
  const esc = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return text.replace(new RegExp(`(${esc})`, 'gi'), '<mark>$1</mark>');
}

// ─── Expand/collapse all helper ───
function updateExpandAllBtn() {
  const btn = document.getElementById('expandAllBtn');
  if (!btn || !store.schemes.length) return;
  const allCollapsed = store.schemes.every(s => collapsedSchemes.has(s.scheme_id));
  btn.textContent = allCollapsed ? 'Expand all' : 'Collapse all';
}

// ─── Render: Facet sidebar ───
function renderFacets() {
  const container = document.getElementById('facets');
  const hasFilters = Object.values(activeFilters).some(s => s.size > 0);
  const hasActive = hasFilters || searchQuery.length > 0;
  document.getElementById('clearFilters').classList.toggle('visible', hasActive);

  container.innerHTML = store.schemes.map(scheme => {
    const values = valuesForScheme(scheme.scheme_id);
    const isActive = activeFilters[scheme.scheme_id] && activeFilters[scheme.scheme_id].size > 0;
    const isCollapsed = collapsedSchemes.has(scheme.scheme_id);

    return `
      <div class="facet-group">
        <div class="facet-header ${isCollapsed ? 'collapsed' : ''}" data-scheme="${scheme.scheme_id}">
          <h3>${scheme.name}</h3>
          <svg class="chevron" width="12" height="12" viewBox="0 0 12 12"><path d="M4 2l4 4-4 4" stroke="currentColor" stroke-width="1.5" fill="none" stroke-linecap="round"/></svg>
        </div>
        <div class="facet-list ${isCollapsed ? 'collapsed' : ''}" data-scheme="${scheme.scheme_id}">
          ${values.map(v => {
            const count = getValueCountForFiltered(v.value_id, scheme.scheme_id);
            const active = activeFilters[scheme.scheme_id] && activeFilters[scheme.scheme_id].has(v.value_id);
            if (!showZeros && count === 0 && !active) return '';
            return `
              <div class="facet-item ${active ? 'active' : ''}" data-value="${v.value_id}" data-scheme="${scheme.scheme_id}" title="${v.description}">
                <div class="facet-check"></div>
                <span class="facet-label">${v.label}</span>
                <span class="facet-count ${count === 0 ? 'zero' : ''}">${count}</span>
              </div>`;
          }).join('')}
        </div>
      </div>`;
  }).join('');

  // Event delegation
  container.querySelectorAll('.facet-header').forEach(h => {
    h.addEventListener('click', () => {
      const schemeId = h.dataset.scheme;
      if (collapsedSchemes.has(schemeId)) {
        collapsedSchemes.delete(schemeId);
      } else {
        collapsedSchemes.add(schemeId);
      }
      h.classList.toggle('collapsed');
      h.nextElementSibling.classList.toggle('collapsed');
      updateExpandAllBtn();
    });
  });
  updateExpandAllBtn();

  container.querySelectorAll('.facet-item').forEach(item => {
    item.addEventListener('click', () => {
      const schemeId = item.dataset.scheme;
      const valueId = item.dataset.value;
      if (!activeFilters[schemeId]) activeFilters[schemeId] = new Set();
      if (activeFilters[schemeId].has(valueId)) {
        activeFilters[schemeId].delete(valueId);
      } else {
        activeFilters[schemeId].add(valueId);
      }
      renderAll();
    });
  });

  const showZerosBtn = document.getElementById('showZerosBtn');
  if (showZerosBtn) {
    showZerosBtn.disabled = store.bcs.length === 0;
    showZerosBtn.textContent = showZeros ? 'Hide zeros' : 'Show zeros';
    showZerosBtn.classList.toggle('on', showZeros);
    showZerosBtn.title = showZeros
      ? 'Currently showing all classification values, including those with 0 BCs. Click to hide them.'
      : 'Some classification values are hidden because they have 0 matching BCs. Click to reveal them.';
  }
}

// ─── Render: Explorer view ───
function renderExplorer() {
  const container = document.getElementById('explorerView');
  const bcs = getFilteredBcs();

  container.innerHTML = `<div class="bc-grid">${bcs.map((bc, i) => {
    const asgns = assignmentsForBc(bc.bc_id);
    const tags = asgns.map(a => {
      const v = valueById(a.value_id);
      return v ? `<span class="bc-tag scheme-${a.scheme_id}">${highlight(v.label, searchQuery)}</span>` : '';
    }).join('');

    return `
      <div class="bc-card ${selectedBcId === bc.bc_id ? 'selected' : ''}" data-bc="${bc.bc_id}" style="--i:${i}">
        <div class="bc-card-id">${highlight(bc.bc_id, searchQuery)}</div>
        <div class="bc-card-name">${highlight(bc.short_name, searchQuery)}</div>
        <div class="bc-card-tags">${tags}</div>
      </div>`;
  }).join('')}</div>`;

  container.querySelectorAll('.bc-card').forEach(card => {
    card.addEventListener('click', () => openDetail(card.dataset.bc));
  });
}

// ─── Render: Browse view (grouped by scheme, then value) ───
function renderMatrix() {
  const container = document.getElementById('matrixView');
  const filteredBcIds = new Set(getFilteredBcs().map(b => b.bc_id));

  const schemeBlocks = store.schemes.map(scheme => {
    const values = valuesForScheme(scheme.scheme_id);

    const valueRows = values.map(v => {
      const bcs = bcsForValue(v.value_id).filter(b => filteredBcIds.has(b.bc_id));
      if (bcs.length === 0) return '';
      return `
        <div class="grouped-value-row">
          <div class="grouped-value-meta" data-scheme="${scheme.scheme_id}" data-value="${v.value_id}" title="${v.description} — click to filter Explorer">
            <span class="grouped-value-name">${v.label}</span>
            <span class="facet-count">${bcs.length}</span>
            <span class="grouped-value-arrow">→</span>
          </div>
          <div class="grouped-bc-chips">
            ${bcs.map(bc => `<span class="bc-chip ${selectedBcId === bc.bc_id ? 'selected' : ''}" data-bc="${bc.bc_id}">${highlight(bc.short_name, searchQuery)}</span>`).join('')}
          </div>
        </div>`;
    }).join('');

    if (!valueRows.trim()) return '';
    return `
      <div class="grouped-scheme">
        <div class="grouped-scheme-header">
          <h3>${scheme.name}</h3>
          <p>${scheme.purpose}</p>
        </div>
        <div class="grouped-scheme-body">${valueRows}</div>
      </div>`;
  }).join('');

  container.innerHTML = `<div class="grouped-list">${schemeBlocks}</div>`;

  container.querySelectorAll('.bc-chip').forEach(chip => {
    chip.addEventListener('click', () => openDetail(chip.dataset.bc));
  });

  container.querySelectorAll('.grouped-value-meta').forEach(meta => {
    meta.addEventListener('click', () => {
      const schemeId = meta.dataset.scheme;
      const valueId = meta.dataset.value;
      if (!activeFilters[schemeId]) activeFilters[schemeId] = new Set();
      activeFilters[schemeId].clear();
      activeFilters[schemeId].add(valueId);
      switchView('explorer');
      renderAll();
    });
  });
}

// ─── Detail panel ───
function openDetail(bcId) {
  selectedBcId = bcId;
  const bc = bcById(bcId);
  if (!bc) return;

  const asgns = assignmentsForBc(bcId);
  const panel = document.getElementById('detailPanel');
  const content = document.getElementById('detailContent');

  // Group assignments by scheme
  const grouped = {};
  asgns.forEach(a => {
    if (!grouped[a.scheme_id]) grouped[a.scheme_id] = [];
    grouped[a.scheme_id].push(a);
  });

  // Find related BCs (share at least one value)
  const myValues = new Set(asgns.map(a => a.value_id));
  const relatedIds = new Set();
  store.assignments.forEach(a => {
    if (a.bc_id !== bcId && myValues.has(a.value_id)) relatedIds.add(a.bc_id);
  });

  content.innerHTML = `
    <div class="detail-id">${bc.ncit_code}</div>
    <div class="detail-name">${bc.short_name}</div>
    <div class="detail-rule"></div>
    ${store.schemes.map(scheme => {
      const vals = grouped[scheme.scheme_id] || [];
      if (vals.length === 0) return '';
      return `
        <div class="detail-section">
          <div class="detail-section-title">${scheme.name}</div>
          ${vals.map(a => {
            const v = valueById(a.value_id);
            return v ? `<div class="detail-value">
              <div class="detail-value-label">${v.label}</div>
              <div class="detail-value-desc">${v.description}</div>
            </div>` : '';
          }).join('')}
        </div>`;
    }).join('')}
    <div class="detail-section">
      <div class="detail-section-title">Related BCs (${relatedIds.size})</div>
      <div>${[...relatedIds].map(rid => {
        const r = bcById(rid);
        return r ? `<span class="related-bc" data-bc="${rid}">${r.short_name}</span>` : '';
      }).join('')}</div>
    </div>`;

  panel.classList.add('open');

  content.querySelectorAll('.related-bc').forEach(el => {
    el.addEventListener('click', () => openDetail(el.dataset.bc));
  });

  // Update card / chip selection
  document.querySelectorAll('.bc-card, .bc-chip').forEach(c => c.classList.toggle('selected', c.dataset.bc === bcId));
}

function closeDetail() {
  selectedBcId = null;
  document.getElementById('detailPanel').classList.remove('open');
  document.querySelectorAll('.bc-card.selected, .bc-chip.selected').forEach(c => c.classList.remove('selected'));
}

// ─── View switching ───
function switchView(view) {
  currentView = view;
  document.querySelectorAll('.view-tab').forEach(t => t.classList.toggle('active', t.dataset.view === view));
  document.getElementById('explorerView').classList.toggle('hidden', view !== 'explorer');
  document.getElementById('matrixView').classList.toggle('hidden', view !== 'matrix');
  document.getElementById('sidebar').style.display = '';
}

// ─── Master render ───
function renderAll() {
  const count = getFilteredBcs().length;
  document.getElementById('searchCount').textContent = `${count} of ${store.bcs.length} BCs`;
  renderFacets();
  if (currentView === 'explorer') renderExplorer();
  else if (currentView === 'matrix') renderMatrix();
}

// ─── Event wiring ───
// NOTE: this script is injected dynamically by an HTMX swap, well after
// the page's own DOMContentLoaded has already fired — so it must not
// wait for that event (it would never fire again). By the time this
// script runs, the explorer's markup (from bc_explorer.html) is already
// in the DOM, since HTMX swaps the HTML in before executing scripts
// found within it.
(() => {
  // View tabs
  document.querySelectorAll('.view-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      switchView(tab.dataset.view);
      renderAll();
    });
  });

  // Search
  document.getElementById('globalSearch').addEventListener('input', e => {
    searchQuery = e.target.value.trim();
    document.getElementById('searchClearBtn').classList.toggle('hidden', !searchQuery);
    renderAll();
  });

  // Clear search ✕
  document.getElementById('searchClearBtn').addEventListener('click', () => {
    searchQuery = '';
    document.getElementById('globalSearch').value = '';
    document.getElementById('searchClearBtn').classList.add('hidden');
    renderAll();
  });

  // Search scope toggle
  document.getElementById('searchScopeBtn').addEventListener('click', () => {
    searchAllProperties = !searchAllProperties;
    const btn = document.getElementById('searchScopeBtn');
    btn.textContent = searchAllProperties ? 'All fields' : 'Names only';
    btn.classList.toggle('on', searchAllProperties);
    if (searchQuery) renderAll();
  });

  // Clear all (filters + search)
  document.getElementById('clearFilters').addEventListener('click', () => {
    activeFilters = {};
    searchQuery = '';
    document.getElementById('globalSearch').value = '';
    document.getElementById('searchClearBtn').classList.add('hidden');
    renderAll();
  });

  // Show / hide zero-count facet values
  document.getElementById('showZerosBtn').addEventListener('click', () => {
    showZeros = !showZeros;
    renderFacets();
  });

  // Expand / collapse all facets
  document.getElementById('expandAllBtn').addEventListener('click', () => {
    const allCollapsed = store.schemes.every(s => collapsedSchemes.has(s.scheme_id));
    if (allCollapsed) {
      collapsedSchemes.clear();
    } else {
      store.schemes.forEach(s => collapsedSchemes.add(s.scheme_id));
    }
    renderFacets();
  });

  // Close detail
  document.getElementById('detailClose').addEventListener('click', closeDetail);
  document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDetail(); });

  switchView('explorer');
  renderAll();
})();
