"""Deterministic retrieval augmentation, separate from quoted source evidence."""
import hashlib


def material(source, illustrations):
    parts = [source] if source else []
    for item in illustrations:
        caption = item.get('caption_text') or ''
        description = item.get('visual_description') or ''
        if caption:
            parts.append('[Printed caption]\n' + caption)
        if description:
            parts.append('[Model observation; not printed source text]\n' + description)
    text = '\n\n'.join(parts)
    if not text.strip() or len(text) > 40000:
        raise ValueError('search material must contain 1..40000 characters')
    return text, hashlib.sha256(text.encode()).hexdigest()


def graph_material(graph, chunk):
    regions = {str(r.region_id): r for p in graph.pages for r in p.regions}
    figures = {str(i.illustration_id): i for i in graph.illustrations}
    descriptions = []
    for iid in chunk.illustration_ids:
        figure = figures[str(iid)]
        descriptions.append({'caption_text': '\n'.join(regions[str(r)].source_text for r in figure.caption_region_ids),
                             'visual_description': figure.visual_description})
    return material(chunk.text, descriptions)
