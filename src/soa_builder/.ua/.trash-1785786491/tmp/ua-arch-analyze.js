#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

function fail(msg) {
  process.stderr.write('ERROR: ' + msg + '\n');
  process.exit(1);
}

const inputPath = process.argv[2];
const outputPath = process.argv[3];
if (!inputPath || !outputPath) {
  fail('Usage: ua-arch-analyze.js <input.json> <output.json>');
}

let input;
try {
  input = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
} catch (e) {
  fail('Failed to read/parse input: ' + e.message);
}

const fileNodes = input.fileNodes || [];
const importEdges = input.importEdges || [];
const allEdges = input.allEdges || [];

const nodeById = new Map();
for (const n of fileNodes) nodeById.set(n.id, n);

// ---------- A. Directory Grouping ----------
function dirOf(fp) {
  const idx = fp.lastIndexOf('/');
  return idx === -1 ? '' : fp.substring(0, idx);
}

const filePaths = fileNodes.map((n) => n.filePath || n.name || '');

function commonPrefix(paths) {
  if (paths.length === 0) return '';
  const split = paths.map((p) => p.split('/'));
  const minLen = Math.min(...split.map((s) => s.length));
  const prefixParts = [];
  for (let i = 0; i < minLen - 1; i++) {
    const seg = split[0][i];
    if (split.every((s) => s[i] === seg)) {
      prefixParts.push(seg);
    } else {
      break;
    }
  }
  return prefixParts.length ? prefixParts.join('/') + '/' : '';
}

const prefix = commonPrefix(filePaths.filter(Boolean));

function groupKeyFor(node) {
  const fp = node.filePath || node.name || '';
  let rest = fp;
  if (prefix && fp.startsWith(prefix)) {
    rest = fp.substring(prefix.length);
  }
  const parts = rest.split('/');
  if (parts.length > 1) {
    return parts[0];
  }
  // flat file directly under prefix (or no prefix at all) -> group by ext pattern
  const base = parts[0] || fp;
  if (/\.test\.|\.spec\.|^test_|_test\.|Test\.|_spec\./.test(base)) return 'test';
  if (/\.config\.|^config/.test(base)) return 'config';
  const ext = base.includes('.') ? base.substring(base.lastIndexOf('.') + 1) : 'other';
  return ext || 'root';
}

const directoryGroups = {};
const groupKeyByNodeId = new Map();
for (const n of fileNodes) {
  const g = groupKeyFor(n);
  groupKeyByNodeId.set(n.id, g);
  if (!directoryGroups[g]) directoryGroups[g] = [];
  directoryGroups[g].push(n.id);
}

// ---------- B. Node Type Grouping ----------
const nodeTypeGroups = {};
for (const n of fileNodes) {
  const t = n.type || 'file';
  if (!nodeTypeGroups[t]) nodeTypeGroups[t] = [];
  nodeTypeGroups[t].push(n.id);
}

// ---------- C. Import Adjacency + fan-in/out ----------
const fanOut = {};
const fanIn = {};
const adjacency = {}; // source -> set(targets)

for (const e of importEdges) {
  if (!nodeById.has(e.source) || !nodeById.has(e.target)) continue;
  fanOut[e.source] = (fanOut[e.source] || 0) + 1;
  fanIn[e.target] = (fanIn[e.target] || 0) + 1;
  if (!adjacency[e.source]) adjacency[e.source] = new Set();
  adjacency[e.source].add(e.target);
}

// ---------- D. Cross-Category Dependency Analysis ----------
const crossCategoryMap = new Map();
for (const e of allEdges) {
  const s = nodeById.get(e.source);
  const t = nodeById.get(e.target);
  if (!s || !t) continue;
  const fromType = s.type || 'file';
  const toType = t.type || 'file';
  if (fromType === toType) continue; // only cross-category
  const key = fromType + '|' + toType + '|' + e.type;
  crossCategoryMap.set(key, (crossCategoryMap.get(key) || 0) + 1);
}
const crossCategoryEdges = [];
for (const [key, count] of crossCategoryMap.entries()) {
  const [fromType, toType, edgeType] = key.split('|');
  crossCategoryEdges.push({ fromType, toType, edgeType, count });
}

// ---------- E. Inter-Group Import Frequency ----------
const interGroupMap = new Map();
for (const e of importEdges) {
  if (!nodeById.has(e.source) || !nodeById.has(e.target)) continue;
  const gFrom = groupKeyByNodeId.get(e.source);
  const gTo = groupKeyByNodeId.get(e.target);
  if (!gFrom || !gTo || gFrom === gTo) continue;
  const key = gFrom + '->' + gTo;
  interGroupMap.set(key, (interGroupMap.get(key) || 0) + 1);
}
const interGroupImports = [];
for (const [key, count] of interGroupMap.entries()) {
  const [from, to] = key.split('->');
  interGroupImports.push({ from, to, count });
}

// ---------- F. Intra-Group Import Density ----------
const intraGroupDensity = {};
for (const g of Object.keys(directoryGroups)) {
  let internalEdges = 0;
  let totalEdges = 0;
  for (const e of importEdges) {
    if (!nodeById.has(e.source) || !nodeById.has(e.target)) continue;
    const gFrom = groupKeyByNodeId.get(e.source);
    const gTo = groupKeyByNodeId.get(e.target);
    if (gFrom === g || gTo === g) {
      totalEdges++;
      if (gFrom === g && gTo === g) internalEdges++;
    }
  }
  intraGroupDensity[g] = {
    internalEdges,
    totalEdges,
    density: totalEdges > 0 ? internalEdges / totalEdges : 0,
  };
}

// ---------- G. Directory Pattern Matching ----------
const dirPatternTable = [
  { pats: ['routes', 'api', 'controllers', 'endpoints', 'handlers'], label: 'api' },
  { pats: ['services', 'core', 'lib', 'domain', 'logic'], label: 'service' },
  { pats: ['models', 'db', 'data', 'persistence', 'repository', 'entities'], label: 'data' },
  { pats: ['components', 'views', 'pages', 'ui', 'layouts', 'screens'], label: 'ui' },
  { pats: ['middleware', 'plugins', 'interceptors', 'guards'], label: 'middleware' },
  { pats: ['utils', 'helpers', 'common', 'shared', 'tools'], label: 'utility' },
  { pats: ['config', 'constants', 'env', 'settings'], label: 'config' },
  { pats: ['__tests__', 'test', 'tests', 'spec', 'specs'], label: 'test' },
  { pats: ['types', 'interfaces', 'schemas', 'contracts', 'dtos'], label: 'types' },
  { pats: ['hooks'], label: 'hooks' },
  { pats: ['store', 'state', 'reducers', 'actions', 'slices'], label: 'state' },
  { pats: ['assets', 'static', 'public'], label: 'assets' },
  { pats: ['migrations'], label: 'data' },
  { pats: ['management', 'commands'], label: 'config' },
  { pats: ['templatetags'], label: 'utility' },
  { pats: ['signals'], label: 'service' },
  { pats: ['serializers'], label: 'api' },
  { pats: ['cmd'], label: 'entry' },
  { pats: ['internal'], label: 'service' },
  { pats: ['pkg'], label: 'utility' },
  { pats: ['dto', 'request', 'response'], label: 'types' },
  { pats: ['entity'], label: 'data' },
  { pats: ['controller'], label: 'api' },
  { pats: ['routers'], label: 'api' },
  { pats: ['composables'], label: 'service' },
  { pats: ['blueprints'], label: 'api' },
  { pats: ['mailers', 'jobs', 'channels'], label: 'service' },
  { pats: ['bin'], label: 'entry' },
  { pats: ['docs', 'documentation', 'wiki'], label: 'documentation' },
  { pats: ['deploy', 'deployment', 'infra', 'infrastructure'], label: 'infrastructure' },
  { pats: ['.github', '.gitlab', '.circleci'], label: 'ci-cd' },
  { pats: ['k8s', 'kubernetes', 'helm', 'charts'], label: 'infrastructure' },
  { pats: ['terraform', 'tf'], label: 'infrastructure' },
  { pats: ['docker'], label: 'infrastructure' },
  { pats: ['sql', 'database', 'schema'], label: 'data' },
  { pats: ['templates'], label: 'ui' },
  { pats: ['static'], label: 'assets' },
];

function matchPattern(dirName) {
  const lower = dirName.toLowerCase();
  for (const row of dirPatternTable) {
    if (row.pats.includes(lower)) return row.label;
  }
  return null;
}

const patternMatches = {};
for (const g of Object.keys(directoryGroups)) {
  const m = matchPattern(g);
  if (m) patternMatches[g] = m;
}

// ---------- H. Deployment Topology Detection ----------
const infraFiles = [];
let hasDockerfile = false;
let hasCompose = false;
let hasK8s = false;
let hasTerraform = false;
let hasCI = false;

for (const n of fileNodes) {
  const fp = (n.filePath || '').toLowerCase();
  const base = path.basename(fp);
  if (/dockerfile/.test(base)) {
    hasDockerfile = true;
    infraFiles.push(n.filePath);
  }
  if (/docker-compose/.test(base)) {
    hasCompose = true;
    infraFiles.push(n.filePath);
  }
  if (/\.ya?ml$/.test(base) && /(k8s|kubernetes|helm)/.test(fp)) {
    hasK8s = true;
    infraFiles.push(n.filePath);
  }
  if (/\.tf$|\.tfvars$/.test(base)) {
    hasTerraform = true;
    infraFiles.push(n.filePath);
  }
  if (/\.github\/workflows\//.test(fp) || /\.gitlab-ci\.yml/.test(base) || /jenkinsfile/.test(base)) {
    hasCI = true;
    infraFiles.push(n.filePath);
  }
}

const deploymentTopology = {
  hasDockerfile,
  hasCompose,
  hasK8s,
  hasTerraform,
  hasCI,
  infraFiles: [...new Set(infraFiles)],
};

// ---------- I. Data Pipeline Detection ----------
const schemaFiles = [];
const migrationFiles = [];
const dataModelFiles = [];
const apiHandlerFiles = [];

for (const n of fileNodes) {
  const fp = n.filePath || '';
  const lower = fp.toLowerCase();
  if (/\.sql$/.test(lower) || /\.graphql$|\.gql$|\.proto$/.test(lower)) {
    schemaFiles.push(fp);
  }
  if (/migrat/.test(lower)) {
    migrationFiles.push(fp);
  }
  const g = groupKeyByNodeId.get(n.id);
  if (patternMatches[g] === 'data') dataModelFiles.push(fp);
  if (patternMatches[g] === 'api') apiHandlerFiles.push(fp);
}

const dataPipeline = {
  schemaFiles: [...new Set(schemaFiles)],
  migrationFiles: [...new Set(migrationFiles)],
  dataModelFiles: [...new Set(dataModelFiles)],
  apiHandlerFiles: [...new Set(apiHandlerFiles)],
};

// ---------- J. Documentation Coverage ----------
const groupsWithDocsSet = new Set();
const docNodes = fileNodes.filter((n) => n.type === 'document' || /\.md$|\.rst$/i.test(n.filePath || ''));
for (const g of Object.keys(directoryGroups)) {
  const hasReadme = directoryGroups[g].some((id) => {
    const n = nodeById.get(id);
    return n && /readme/i.test(path.basename(n.filePath || ''));
  });
  const hasDocRef = docNodes.some((d) => (d.filePath || '').toLowerCase().includes(g.toLowerCase()));
  if (hasReadme || hasDocRef) groupsWithDocsSet.add(g);
}
const totalGroups = Object.keys(directoryGroups).length;
const groupsWithDocs = groupsWithDocsSet.size;
const undocumentedGroups = Object.keys(directoryGroups).filter((g) => !groupsWithDocsSet.has(g));

const docCoverage = {
  groupsWithDocs,
  totalGroups,
  coverageRatio: totalGroups > 0 ? groupsWithDocs / totalGroups : 0,
  undocumentedGroups,
};

// ---------- K. Dependency Direction ----------
const dependencyDirection = [];
const seenPairs = new Set();
for (const { from, to, count } of interGroupImports) {
  const pairKey = [from, to].sort().join('|');
  if (seenPairs.has(pairKey)) continue;
  seenPairs.add(pairKey);
  const reverse = interGroupImports.find((x) => x.from === to && x.to === from);
  const reverseCount = reverse ? reverse.count : 0;
  if (count > reverseCount) {
    dependencyDirection.push({ dependent: from, dependsOn: to });
  } else if (reverseCount > count) {
    dependencyDirection.push({ dependent: to, dependsOn: from });
  }
}

// ---------- fileStats ----------
const filesPerGroup = {};
for (const g of Object.keys(directoryGroups)) filesPerGroup[g] = directoryGroups[g].length;

const nodeTypeCounts = {};
for (const t of Object.keys(nodeTypeGroups)) nodeTypeCounts[t] = nodeTypeGroups[t].length;

const fileStats = {
  totalFileNodes: fileNodes.length,
  filesPerGroup,
  nodeTypeCounts,
};

const result = {
  scriptCompleted: true,
  directoryGroups,
  nodeTypeGroups,
  crossCategoryEdges,
  interGroupImports,
  intraGroupDensity,
  patternMatches,
  deploymentTopology,
  dataPipeline,
  docCoverage,
  dependencyDirection,
  fileStats,
  fileFanIn: fanIn,
  fileFanOut: fanOut,
  commonPrefix: prefix,
};

try {
  fs.writeFileSync(outputPath, JSON.stringify(result, null, 2));
} catch (e) {
  fail('Failed to write output: ' + e.message);
}

process.exit(0);
