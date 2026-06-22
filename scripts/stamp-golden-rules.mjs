#!/usr/bin/env node
/**
 * stamp-golden-rules.mjs — estampa/actualiza el banner de Reglas de Oro IMAP en
 * cada archivo fuente, desde la fuente única golden-rules.json.
 *
 * Uso:
 *   node scripts/stamp-golden-rules.mjs            → estampa (escribe) todo src/
 *   node scripts/stamp-golden-rules.mjs --check    → NO escribe; falla (exit 1) si hay drift
 *   node scripts/stamp-golden-rules.mjs <archivos> → solo esos archivos
 *
 * El banner va entre marcadores GOLDEN-RULES:BEGIN/END. Idempotente: re-estampar
 * solo reemplaza esa región. Las reglas aplicadas a un archivo = unión de los
 * rulesets cuyos globs matchean su path (ver apply[] en golden-rules.json).
 */
import { readFileSync, writeFileSync, readdirSync, statSync, existsSync } from 'node:fs';
import { join, relative, extname } from 'node:path';

const ROOT = process.cwd();
const CFG = JSON.parse(readFileSync(join(ROOT, 'golden-rules.json'), 'utf8'));
const CHECK = process.argv.includes('--check');
const fileArgs = process.argv.slice(2).filter((a) => !a.startsWith('--'));

const COMMENT = { '.java': '//', '.ts': '//', '.tsx': '//', '.js': '//', '.mjs': '//', '.sql': '--' };
const EXTS = Object.keys(COMMENT);
const SKIP = new Set(['.git', 'node_modules', 'target', 'build', 'dist', '.idea', 'scripts']);

function walk(dir, acc) {
  for (const name of readdirSync(dir)) {
    if (SKIP.has(name)) continue;
    const p = join(dir, name);
    const st = statSync(p);
    if (st.isDirectory()) walk(p, acc);
    else if (EXTS.includes(extname(name))) acc.push(p);
  }
  return acc;
}

function globToRe(glob) {
  let re = '';
  glob = glob.replace(/\\/g, '/');
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i];
    if (c === '*') {
      if (glob[i + 1] === '*') { re += '.*'; i++; if (glob[i + 1] === '/') i++; }
      else re += '[^/]*';
    } else if ('.+?^${}()|[]\\'.includes(c)) re += '\\' + c;
    else re += c;
  }
  return new RegExp('^' + re + '$');
}
const APPLY = CFG.apply.map((a) => ({ res: a.globs.map(globToRe), rules: a.rules }));

function rulesetsFor(relPath) {
  const p = relPath.replace(/\\/g, '/');
  const sets = [];
  for (const a of APPLY) {
    if (a.res.some((re) => re.test(p))) for (const r of a.rules) if (!sets.includes(r)) sets.push(r);
  }
  return sets;
}

function renderBanner(prefix, sets) {
  const out = [];
  out.push(`${prefix} ─── GOLDEN-RULES:BEGIN (auto · golden-rules.json · no editar a mano) ───`);
  out.push(`${prefix} REGLAS DE ORO IMAP — cumplir SIEMPRE (ver ${CFG.doc}):`);
  for (const set of sets) {
    const tag = set === 'global' ? '' : `[${set}] `;
    for (const rule of (CFG.rulesets[set] || [])) out.push(`${prefix}  • ${tag}${rule}`);
  }
  out.push(`${prefix} ─── GOLDEN-RULES:END ───`);
  return out;
}

const BEGIN = /GOLDEN-RULES:BEGIN/;
const END = /GOLDEN-RULES:END/;

// Migraciones Flyway son INMUTABLES: tocarlas (aunque sea un comentario) rompe el
// checksum y el micro no arranca. NUNCA estampar migraciones.
function isMigration(rel) {
  const u = rel.replace(/\\/g, '/');
  return /\/migration\//i.test(u) || /(^|\/)[VRU][0-9._]*__.+\.sql$/i.test(u);
}

function stampFile(file) {
  const rel = relative(ROOT, file);
  if (isMigration(rel)) return null;
  const sets = rulesetsFor(rel);
  if (sets.length === 0) return null;
  const prefix = COMMENT[extname(file)];
  const bannerLines = renderBanner(prefix, sets);
  const raw = readFileSync(file, 'utf8');
  const eol = raw.includes('\r\n') ? '\r\n' : '\n';
  const lines = raw.split(/\r?\n/);
  const b = lines.findIndex((l) => BEGIN.test(l));
  const e = lines.findIndex((l) => END.test(l));
  let updated;
  if (b !== -1 && e !== -1 && e >= b) {
    updated = [...lines.slice(0, b), ...bannerLines, ...lines.slice(e + 1)];
  } else {
    updated = [...bannerLines, '', ...lines];
  }
  const newRaw = updated.join(eol);
  const changed = newRaw !== raw;
  if (changed && !CHECK) writeFileSync(file, newRaw);
  return { rel, changed };
}

const base = fileArgs.length ? fileArgs.map((f) => join(ROOT, f))
  : (existsSync(join(ROOT, 'src')) ? walk(join(ROOT, 'src'), []) : walk(ROOT, []));
let drift = 0;
for (const f of base) {
  const r = stampFile(f);
  if (r && r.changed) { drift++; console.log(`${CHECK ? 'DRIFT' : 'stamped'}: ${r.rel}`); }
}
if (CHECK && drift > 0) {
  console.error(`\n✗ ${drift} archivo(s) con banner ausente/desactualizado — corré: node scripts/stamp-golden-rules.mjs`);
  process.exit(1);
}
console.log(CHECK ? '✓ banners OK' : `✓ ${drift} estampado(s)`);
