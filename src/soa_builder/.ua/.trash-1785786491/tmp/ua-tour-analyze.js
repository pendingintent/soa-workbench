#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

function main() {
  const inputPath = process.argv[2];
  const outputPath = process.argv[3];
  if (!inputPath || !outputPath) {
    console.error('Usage: node ua-tour-analyze.js <input.json> <output.json>');
    process.exit(1);
  }

  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  } catch (e) {
    console.error('Failed to read/parse input JSON: ' + e.message);
    process.exit(1);
  }

  const nodes = raw.nodes || [];
  const edges = raw.edges || [];
  const layers = raw.layers || [];

  const nodeById = new Map();
  for (const n of nodes) {
    nodeById.set(n.id, n);
  }

  // Fan-in / fan-out counts
  const fanIn = new Map();
  const fanOut = new Map();
  for (const n of nodes) {
    fanIn.set(n.id, 0);
    fanOut.set(n.id, 0);
  }
  // adjacency for imports/calls only (for BFS)
  const importsCallsAdj = new Map();
  for (const n of nodes) importsCallsAdj.set(n.id, []);

  for (const e of edges) {
    if (nodeById.has(e.source)) {
      fanOut.set(e.source, (fanOut.get(e.source) || 0) + 1);
    }
    if (nodeById.has(e.target)) {
      fanIn.set(e.target, (fanIn.get(e.target) || 0) + 1);
    }
    if ((e.type === 'imports' || e.type === 'calls') &&
        nodeById.has(e.source) && nodeById.has(e.target)) {
      importsCallsAdj.get(e.source).push(e.target);
    }
  }

  const fanInRanking = nodes
    .map((n) => ({ id: n.id, fanIn: fanIn.get(n.id) || 0, name: n.name }))
    .sort((a, b) => b.fanIn - a.fanIn)
    .slice(0, 20);

  const fanOutRanking = nodes
    .map((n) => ({ id: n.id, fanOut: fanOut.get(n.id) || 0, name: n.name }))
    .sort((a, b) => b.fanOut - a.fanOut)
    .slice(0, 20);

  // Percentile thresholds for fan-out (top 10%) and fan-in (bottom 25%)
  const fanOutValues = nodes.map((n) => fanOut.get(n.id) || 0).sort((a, b) => a - b);
  const fanInValues = nodes.map((n) => fanIn.get(n.id) || 0).sort((a, b) => a - b);
  function percentileValue(sortedArr, p) {
    if (sortedArr.length === 0) return 0;
    const idx = Math.min(
      sortedArr.length - 1,
      Math.floor(p * (sortedArr.length - 1))
    );
    return sortedArr[idx];
  }
  const fanOutTop10Threshold = percentileValue(fanOutValues, 0.9);
  const fanInBottom25Threshold = percentileValue(fanInValues, 0.25);

  const ENTRY_FILENAMES = new Set([
    'index.ts', 'index.js', 'main.ts', 'main.js', 'app.ts', 'app.js',
    'server.ts', 'server.js', 'mod.rs', 'main.go', 'main.py', 'main.rs',
    'manage.py', 'app.py', 'wsgi.py', 'asgi.py', 'run.py', '__main__.py',
    'Application.java', 'Main.java', 'Program.cs', 'config.ru', 'index.php',
    'App.swift', 'Application.kt', 'main.cpp', 'main.c',
  ]);

  function pathDepth(filePath) {
    if (!filePath) return 99;
    const norm = filePath.replace(/^\/+/, '');
    return norm.split('/').filter(Boolean).length;
  }

  const entryScored = [];
  for (const n of nodes) {
    let score = 0;
    const fp = n.filePath || '';
    const base = path.basename(fp || n.name || '');
    if (n.type === 'file') {
      if (ENTRY_FILENAMES.has(base)) score += 3;
      const depth = pathDepth(fp);
      if (depth <= 2) score += 1;
      if ((fanOut.get(n.id) || 0) >= fanOutTop10Threshold && fanOutTop10Threshold > 0) score += 1;
      if ((fanIn.get(n.id) || 0) <= fanInBottom25Threshold) score += 1;
    } else if (n.type === 'document') {
      const isRoot = pathDepth(fp) <= 1;
      if (base.toLowerCase() === 'readme.md' && isRoot) {
        score += 5;
      } else if (base.toLowerCase().endsWith('.md') && isRoot) {
        score += 2;
      }
    }
    if (score > 0) {
      entryScored.push({ id: n.id, score, name: n.name, summary: n.summary });
    }
  }
  entryScored.sort((a, b) => b.score - a.score);
  const entryPointCandidates = entryScored.slice(0, 5);

  // BFS from top code entry point (skip documentation nodes)
  const topCodeEntry = entryScored.find((c) => {
    const node = nodeById.get(c.id);
    return node && node.type !== 'document';
  });

  const bfsTraversal = { startNode: null, order: [], depthMap: {}, byDepth: {} };
  if (topCodeEntry) {
    const start = topCodeEntry.id;
    bfsTraversal.startNode = start;
    const visited = new Set([start]);
    const queue = [[start, 0]];
    bfsTraversal.depthMap[start] = 0;
    while (queue.length > 0) {
      const [cur, depth] = queue.shift();
      bfsTraversal.order.push(cur);
      if (!bfsTraversal.byDepth[depth]) bfsTraversal.byDepth[depth] = [];
      bfsTraversal.byDepth[depth].push(cur);
      const neighbors = importsCallsAdj.get(cur) || [];
      for (const nb of neighbors) {
        if (!visited.has(nb)) {
          visited.add(nb);
          bfsTraversal.depthMap[nb] = depth + 1;
          queue.push([nb, depth + 1]);
        }
      }
    }
  }

  // Non-code file inventory
  const nonCodeFiles = {
    documentation: [],
    infrastructure: [],
    data: [],
    config: [],
  };
  for (const n of nodes) {
    const entry = { id: n.id, name: n.name, type: n.type, summary: n.summary };
    if (n.type === 'document') {
      nonCodeFiles.documentation.push(entry);
    } else if (n.type === 'service' || n.type === 'pipeline' || n.type === 'resource') {
      nonCodeFiles.infrastructure.push(entry);
    } else if (n.type === 'table' || n.type === 'schema' || n.type === 'endpoint') {
      nonCodeFiles.data.push(entry);
    } else if (n.type === 'config') {
      nonCodeFiles.config.push(entry);
    }
  }

  // Tightly coupled clusters
  // Build directed edge set for imports/calls between file-ish nodes
  const edgeSet = new Set();
  const directedAdj = new Map();
  for (const n of nodes) directedAdj.set(n.id, new Set());
  for (const e of edges) {
    if ((e.type === 'imports' || e.type === 'calls') &&
        nodeById.has(e.source) && nodeById.has(e.target) &&
        e.source !== e.target) {
      edgeSet.add(e.source + '->' + e.target);
      directedAdj.get(e.source).add(e.target);
    }
  }

  const bidirectionalPairs = [];
  for (const key of edgeSet) {
    const [a, b] = key.split('->');
    if (edgeSet.has(b + '->' + a) && a < b) {
      bidirectionalPairs.push([a, b]);
    }
  }

  // Union pairs into clusters, then expand
  const clusterMap = new Map(); // node -> cluster set reference
  const clusters = [];
  for (const [a, b] of bidirectionalPairs) {
    let clusterA = clusterMap.get(a);
    let clusterB = clusterMap.get(b);
    if (clusterA && clusterB && clusterA !== clusterB) {
      // merge
      for (const m of clusterB) clusterA.add(m);
      for (const m of clusterB) clusterMap.set(m, clusterA);
    } else if (clusterA) {
      clusterA.add(b);
      clusterMap.set(b, clusterA);
    } else if (clusterB) {
      clusterB.add(a);
      clusterMap.set(a, clusterB);
    } else {
      const newCluster = new Set([a, b]);
      clusters.push(newCluster);
      clusterMap.set(a, newCluster);
      clusterMap.set(b, newCluster);
    }
  }

  // Expand: add nodes connecting to 2+ existing cluster members (cap size at 5)
  const uniqueClusters = Array.from(new Set(clusters));
  for (const cluster of uniqueClusters) {
    let changed = true;
    while (changed && cluster.size < 5) {
      changed = false;
      let bestCandidate = null;
      let bestCount = 0;
      for (const n of nodes) {
        if (cluster.has(n.id)) continue;
        let count = 0;
        for (const member of cluster) {
          if (directedAdj.get(n.id) && directedAdj.get(n.id).has(member)) count++;
          if (directedAdj.get(member) && directedAdj.get(member).has(n.id)) count++;
        }
        if (count >= 2 && count > bestCount) {
          bestCount = count;
          bestCandidate = n.id;
        }
      }
      if (bestCandidate) {
        cluster.add(bestCandidate);
        changed = true;
      }
    }
  }

  function countEdgesWithin(cluster) {
    let count = 0;
    for (const key of edgeSet) {
      const [a, b] = key.split('->');
      if (cluster.has(a) && cluster.has(b)) count++;
    }
    return count;
  }

  const clusterResults = uniqueClusters
    .map((c) => ({ nodes: Array.from(c), edgeCount: countEdgesWithin(c) }))
    .sort((a, b) => b.edgeCount - a.edgeCount)
    .slice(0, 10);

  // Layers
  const layersOut = {
    count: layers.length,
    list: layers.map((l) => ({ id: l.id, name: l.name, description: l.description })),
  };

  // Node summary index
  const nodeSummaryIndex = {};
  for (const n of nodes) {
    nodeSummaryIndex[n.id] = { name: n.name, type: n.type, summary: n.summary };
  }

  const result = {
    scriptCompleted: true,
    entryPointCandidates,
    fanInRanking,
    fanOutRanking,
    bfsTraversal,
    nonCodeFiles,
    clusters: clusterResults,
    layers: layersOut,
    nodeSummaryIndex,
    totalNodes: nodes.length,
    totalEdges: edges.length,
  };

  try {
    fs.writeFileSync(outputPath, JSON.stringify(result, null, 2));
  } catch (e) {
    console.error('Failed to write output JSON: ' + e.message);
    process.exit(1);
  }

  process.exit(0);
}

main();
