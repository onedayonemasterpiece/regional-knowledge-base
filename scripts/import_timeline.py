#!/usr/bin/env python3
"""Read bounded telemetry segments; '?' means unknown, never Saved failure."""
from __future__ import annotations
import argparse, csv, io, json
from pathlib import Path
from statistics import median
COLUMNS = ('page', 'book_pages_ms', 'image_bytes', 'stage_ms', 'packet_bytes',
           'regions', 'chunks', 'unit_markers', 'story_candidates', 'stage_accepted',
           'stage_applied', 'text_applied', 'semantic_applied', 'saved',
           'status_observed', 'fetch_observed', 'gap_before_stage_ms',
           'text_lag_ms', 'semantic_lag_ms', 'next_action', 'outcome')
def load(directory):
    records, bad = [], 0
    for path in sorted(Path(directory).rglob('segment-*')):
        if path.suffix not in ('.open', '.jsonl') or not path.is_file(): continue
        with path.open(encoding='utf-8', errors='replace') as stream:
            for line in stream:
                try:
                    if len(line) > 4096: raise ValueError('oversized')
                    data = json.loads(line)
                    if type(data) is dict and data.get('schema') == 'rkb.product.telemetry.v1':
                        records.append(data)
                except (ValueError, TypeError): bad += 1
    return records, bad
def stamp(e): return e.get('event_time_unix_ms') or e.get('observed_unix_ms')
def start(e):
    end, duration = stamp(e), e.get('duration_ms')
    if type(e.get('call_started_unix_ms')) is int: return e['call_started_unix_ms']
    return end - duration if type(end) in (int, float) and type(duration) in (int, float) else None
def interval(a, b):
    return round(b - a, 1) if type(a) in (int, float) and type(b) in (int, float) and b >= a else None
def integer(e, key):
    value = e.get(key)
    return value if type(value) is int and value >= 0 else None
def report(events, ingestion_ref=None):
    refs = {e['ingestion_ref'] for e in events if type(e.get('ingestion_ref')) is str and len(e['ingestion_ref']) == 64}
    if not ingestion_ref:
        if len(refs) != 1: raise ValueError('select --ingestion-ref for zero/multiple books')
        ingestion_ref = next(iter(refs))
    if ingestion_ref not in refs: raise ValueError('ingestion_ref absent from selected segments')
    ops = {e.get('operation_ref') for e in events if e.get('ingestion_ref') == ingestion_ref}
    sources = {e.get('source_ref') for e in events if e.get('ingestion_ref') == ingestion_ref} - {None}
    relevant = sorted((e for e in events if e.get('ingestion_ref') == ingestion_ref or
                       (e.get('operation_ref') is not None and e.get('operation_ref') in ops) or
                       e.get('tool') == 'fetch' and e.get('source_ref') in sources),
                      key=lambda e: (stamp(e) or 0, e.get('seq', 0)))
    calls = [e for e in relevant if e.get('event') in ('import.call','import.terminal','fetch.terminal') and e.get('tool') in ('book_ingest','book_pages','fetch')]
    rows, batches, operation_pages, multipage = {}, {}, {}, {}
    def row(index):
        if index not in rows:
            rows[index] = {'page': index, **{k: None for k in COLUMNS if k != 'page'},
                           '_issued': None, '_end': None, '_stage_start': None,
                           '_times': {}, '_saved': set(), '_ops': set()}
        return rows[index]
    admissions = [e for e in relevant if e.get('event') == 'import.phase'
                  and e.get('phase') == 'admission' and e.get('status') == 'staged']
    stage_by_trace = {e.get('trace_id'): e for e in admissions if e.get('component') == 'stage'}
    stage_ops = {e.get('operation_ref') for e in stage_by_trace.values()}
    calls_by_trace = {e.get('trace_id'): e for e in calls if e.get('trace_id')}
    unmapped = 0
    for e in calls:
        page = integer(e, 'physical_page_index')
        if page is None: continue
        value = row(page)
        if e.get('tool') == 'book_pages' and e.get('status') == 'returned':
            value['book_pages_ms'], value['image_bytes'] = e.get('duration_ms'), integer(e, 'image_bytes')
            value['_issued'], value['_end'] = start(e), stamp(e)
        elif e.get('tool') == 'book_ingest' and e.get('command') == 'stage':
            count, batch = integer(e, 'page_count') or 1, integer(e, 'batch_index')
            selected = e.get('physical_page_indices')
            if type(selected) is list and selected and page in selected:
                multipage[page] = selected
            else: unmapped += max(0, count - 1)
            value['stage_ms'], value['_stage_start'] = e.get('duration_ms'), start(e)
            value['gap_before_stage_ms'] = interval(value['_end'], start(e))
            value['next_action'] = e.get('next_action')
            value['outcome'] = e.get('result_state') or e.get('result_status') or e.get('status')
            for target, source in (('regions','region_count'), ('chunks','chunk_count'),
                                   ('unit_markers','unit_marker_count'),
                                   ('story_candidates','story_candidate_count')):
                value[target] = integer(e, source)
            admission = stage_by_trace.get(e.get('trace_id'))
            value['packet_bytes'] = integer(admission, 'packet_bytes') if admission else None
            operation = admission.get('operation_ref') if admission else (
                e.get('operation_ref') if e.get('component') == 'stage' else None)
            if operation:
                operation_pages[operation] = page
                value['_ops'].add(operation)
            if batch is not None and count == 1: batches[batch] = page
    for e in admissions:
        op, component = e.get('operation_ref'), e.get('component')
        call = calls_by_trace.get(e.get('trace_id'), {})
        batch = integer(call, 'batch_index')
        if component in ('text','semantic') and batch in batches and op:
            page = batches[batch]
            operation_pages[op] = page
            row(page)['_ops'].add(op)
    applied = [e for e in relevant if e.get('event') == 'import.phase'
               and e.get('status') == 'applied' and e.get('phase') in ('stage','text','semantic')]
    for e in applied:
        op = e.get('operation_ref')
        page = operation_pages.get(op)
        count = integer(e, 'batch_count')
        if page is None and count is not None: page = batches.get(count - 1)
        if page is not None and op: operation_pages[op] = page; row(page)['_ops'].add(op)
    orphan = 0
    for e in relevant:
        op, phase = e.get('operation_ref'), e.get('phase')
        page = operation_pages.get(op)
        if e.get('event') == 'import.phase' and e.get('status') == 'applied' and phase in ('stage','text','semantic'):
            if page is None:
                orphan += 1
                continue
            value = row(page)
            value[phase + '_applied'] = 'yes'
            value['_times'][phase] = stamp(e)
        if e.get('event') == 'command.phase' and page is not None:
            if phase == 'seal' and e.get('status') == 'acknowledged' and op in stage_ops:
                row(page)['stage_accepted'] = 'yes'
            if phase == 'saved' and e.get('status') == 'saved': row(page)['_saved'].add(op)
    for value in rows.values():
        if all(value.get(k + '_applied') == 'yes' for k in ('stage', 'text', 'semantic')):
            if len(value['_saved'] & value['_ops']) >= 3: value['saved'] = 'yes'
        value['text_lag_ms'] = interval(value['_times'].get('stage'), value['_times'].get('text'))
        value['semantic_lag_ms'] = interval(value['_times'].get('text'), value['_times'].get('semantic'))
        value['status_observed'], value['fetch_observed'] = 0, None
    for e in calls:
        if e.get('tool') == 'fetch':
            pg, end = integer(e,'physical_page_index'), integer(e,'physical_page_end')
            if pg is not None and e.get('source_ref') in sources:
                for index in rows:
                    if pg <= index <= (end if end is not None else pg):
                        rows[index]['fetch_observed']=(rows[index]['fetch_observed'] or 0)+1
            continue
        if e.get('tool') != 'book_ingest' or e.get('command') != 'status': continue
        linked, at = operation_pages.get(e.get('operation_ref')), start(e)
        if linked is None and at is not None:
            eligible = [(v['_issued'], p) for p, v in rows.items()
                        if v['_issued'] is not None and v['_issued'] <= at]
            linked = max(eligible)[1] if eligible else None
        if linked is not None: rows[linked]['status_observed'] += 1
    for first, selected in multipage.items():
        for page in selected:
            if page == first: continue
            target, origin = row(page), row(first)
            for key in ('stage_ms','packet_bytes','stage_accepted','stage_applied',
                        'text_applied','semantic_applied','saved','regions','chunks',
                        'unit_markers','story_candidates','next_action','outcome'):
                target[key] = origin[key]
            target['gap_before_stage_ms'] = interval(target['_end'],origin['_stage_start'])
            target['text_lag_ms'],target['semantic_lag_ms']=origin['text_lag_ms'],origin['semantic_lag_ms']
    losses = max((max(e.get('loss_before', {}).get('dropped', 0),
                      e.get('loss_before', {}).get('failed', 0)) for e in relevant), default=0)
    processes = {e.get('process_ref') for e in events if type(e.get('seq')) is int}
    seqs = [sorted({e['seq'] for e in events if e.get('process_ref')==pid
                   and type(e.get('seq')) is int}) for pid in processes]
    gaps = sum(max(0,b-a-1) for seq in seqs for a,b in zip(seq,seq[1:])) if seqs else None
    idle = sum(g for a, b in zip(calls, calls[1:]) if (g := interval(stamp(a), start(b))) is not None)
    active = sum(e['duration_ms'] for e in calls if type(e.get('duration_ms')) in (int,float))
    first, last = [start(e) for e in calls if start(e) is not None], [stamp(e) for e in calls if stamp(e) is not None]
    elapsed = max(last)-min(first) if first and last else None
    metrics = {}
    for name in ('book_pages_ms','stage_ms','gap_before_stage_ms','text_lag_ms','semantic_lag_ms'):
        samples = sorted(v[name] for v in rows.values() if type(v[name]) in (int,float))
        if samples:
            metrics[name] = {'median': round(median(samples), 1),
                             'p90': round(samples[(9*len(samples)+9)//10-1], 1)}
    summary = {'ingestion_ref': ingestion_ref, 'pages_observed': len(rows),
               'unmapped_in_multipage_stage': unmapped, 'orphan_apply_events': orphan,
               'loss_before': losses, 'seq_gaps': gaps, 'no_seq_visibility': not bool(seqs), 'packet_counts_are_shared': bool(multipage),
               'inter_call_share_pct': round(100*idle/(idle+active), 1) if idle+active else None,
               'pages_per_hour_observed': round(3_600_000*len(rows)/elapsed, 1) if elapsed and elapsed>0 else None,
               'uncorrelated_fetch': sum(e.get('tool') == 'fetch' and
                  (integer(e,'physical_page_index') is None or e.get('source_ref') not in sources) for e in calls),
               'metrics': metrics}
    return [rows[p] for p in sorted(rows)], summary

def render(rows, summary):
    values = [{k: '?' if row.get(k) is None else row[k] for k in COLUMNS} for row in rows]
    out = io.StringIO(); writer = csv.DictWriter(out, fieldnames=COLUMNS)
    writer.writeheader(); writer.writerows(values)
    md = ['| ' + ' | '.join(COLUMNS) + ' |', '|' + '|'.join(['---'] * len(COLUMNS)) + '|']
    md += ['| ' + ' | '.join(str(v[k]) for k in COLUMNS) + ' |' for v in values]
    md += ['', 'Observed only; missing fields, Saved and uncorrelated fetch remain unknown.', '```json', json.dumps(summary, indent=2), '```']
    return out.getvalue(), '\n'.join(md) + '\n'

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory', type=Path); p.add_argument('--ingestion-ref', help='hashed ingestion ID')
    p.add_argument('--csv', type=Path); p.add_argument('--markdown', type=Path)
    a = p.parse_args()
    events, bad = load(a.directory)
    rows, summary = report(events, a.ingestion_ref)
    summary['malformed_lines'] = bad
    csv_text, markdown = render(rows, summary)
    if a.csv: a.csv.write_text(csv_text, encoding='utf-8')
    else: print(csv_text, end='')
    if a.markdown: a.markdown.write_text(markdown, encoding='utf-8')
    else: print(markdown, end='')
if __name__ == '__main__': main()
