"""Real synthetic PDF extraction/geometry; no corpus, inference or delivery."""
import copy
import re
from uuid import UUID, uuid5

import pytest

pymupdf = pytest.importorskip('pymupdf')
from regional_knowledge.contracts import BBox, CatalogMetadata, Region, RegionKind, StartMetadataInput
from regional_knowledge.source_unit_pdf import (
    render_source_unit_pdf, SourceUnitPdfError, SERVICE_CAPTION,
)

SOURCE = '31111111-2222-4333-8444-555555555555'
SAMPLE = 'Ёлка, № 17 — «исторія»: ѣ, І, ѳ, ѵ. Königsberg, ä, ö, ü, ß, ẞ; Straße und Küste.'


def inputs(texts=None):
    texts = texts or [('heading','Точная глава / Deutscher Abschnitt'),('body',SAMPLE)]
    page = str(uuid5(UUID(SOURCE),'page:1:0'))
    regions = [Region(region_id=str(uuid5(UUID(page),f'region:{i}')),page_id=page,
        kind=kind,bbox=BBox(left=0,top=0,right=1000,bottom=1000),reading_order=i,
        source_text=text) for i,(kind,text) in enumerate(texts)]
    spans = [dict(page_id=r.page_id,physical_page_index=0,region_id=r.region_id,
        region_key=str(i),kind=r.kind.value,start=0,end=len(r.source_text))
        for i,r in enumerate(regions)]
    unit = dict(source_id=SOURCE,article_id='chapter-one',title='Точная глава',
        state='complete',source_spans=spans,start_anchor=dict(spans[0],offset=0),
        end_anchor=dict(spans[-1],offset=spans[-1]['end']))
    edition = StartMetadataInput(title='Проверенное издание',authors=['Автор / Verfasser'],
        publication_year=1901,catalog=CatalogMetadata(publisher='Издатель',
        identifiers=[{'ISBN':'verified-isbn'},{'DOI':'verified-doi'}]))
    return dict(unit=unit,regions=regions,accepted_revision=1,edition=edition,
        original_pages=('XVII','18–20'))


def flatten(text):return re.sub(r'\s+','',text)


def render(values):
    data = render_source_unit_pdf(**values)
    return pymupdf.open(stream=data,filetype='pdf')


def author_text(doc):
    content = ''.join(block[4] for page in list(doc)[1:] for block in page.get_text('blocks')
        if block[6] == 0 and 49 <= block[1] < page.rect.height - 43)
    return content.removeprefix('Точная глава\n')


def test_real_pdf_ru_de_source_pasport_embedded_fonts_and_exact_author_sequence():
    values = inputs()
    before = copy.deepcopy(values)
    with render(values) as doc:
        assert len(doc)==2
        first=doc[0].get_text()
        assert flatten(SERVICE_CAPTION) in flatten(first)
        assert all(value in first for value in ['Автор / Verfasser','1901','verified-isbn','verified-doi','XVII','18–20'])
        assert 'Страница PDF' not in first
        assert first.endswith('1\n')
        assert 'Точная глава' in first
        assert 'Точная глава' in doc[1].get_text()
        assert flatten(author_text(doc))==flatten(''.join(r.source_text for r in values['regions']))
        assert '\ufffd' not in author_text(doc)
        for page in doc:
            assert page.rect==pymupdf.paper_rect('a4')
            for word in page.get_text('words'):
                assert 55 <= word[0] < word[2] <= page.rect.width-55
                assert 10 <= word[1] < word[3] <= page.rect.height-15
            for font in page.get_fonts(full=True):
                assert doc.extract_font(font[0])[3]
                assert '/ToUnicode' in doc.xref_object(font[0])
    assert values==before


@pytest.mark.parametrize('paragraphs',[50,250,800])
def test_multipage_short_paragraphs_stay_together_and_heading_keeps_next(paragraphs):
    texts=[]
    for i in range(paragraphs):
        if i%19==0:texts.append(('heading',f'Heading-{i:04d}'))
        texts.append(('body',f'Block-{i:04d} '+SAMPLE+' End-'+f'{i:04d}'))
    values=inputs(texts)
    with render(values) as doc:
        assert len(doc)>=3
        assert flatten(author_text(doc))==flatten(''.join(t for _,t in texts))
        page_texts=[page.get_text() for page in doc]
        for i in range(paragraphs):
            selected=[n for n,text in enumerate(page_texts) if f'Block-{i:04d}' in text]
            ends=[n for n,text in enumerate(page_texts) if f'End-{i:04d}' in text]
            assert selected==ends and len(selected)==1
            if i%19==0:
                heading=[n for n,text in enumerate(page_texts) if f'Heading-{i:04d}' in text]
                assert heading==selected
        if paragraphs==800:assert len(doc)>=30


def test_long_paragraph_flows_with_two_line_minimum_and_first_line_indent():
    # Existing Region has a bounded 8000-char stage counterpart. This block
    # spans several pages without a new semantic paragraph or altered text.
    text=' '.join(f'W{i:04d}' for i in range(1200))
    values=inputs([('body',text)])
    with render(values) as doc:
        assert len(doc)>=3
        assert flatten(author_text(doc))==flatten(text)
        body_pages=list(doc)[1:]
        for page in body_pages:
            lines=[line for block in page.get_text('dict')['blocks'] if 'lines' in block
                for line in block['lines'] if line['bbox'][1]<780]
            assert len(lines)>=2
        words=body_pages[0].get_text('words')
        first=next(w for w in words if w[4]=='W0000')
        assert first[0]>=69
        # Next visual author-text line is flush left, not another paragraph.
        next_line=next(w for w in words if w[1]>first[1]+2 and w[4].startswith('W'))
        assert 55<=next_line[0]<=57


def test_internal_newlines_and_html_are_literal_author_text():
    text='Первая строка.\nВторая строка.\n\n<IMG src="https://invalid.example/private"> & <b>автор</b>'
    with render(inputs([('body',text)])) as doc:
        assert flatten(author_text(doc))==flatten(text)
        assert not any(page.get_images() or page.get_links() for page in doc)
        assert len(doc[1].get_text().splitlines())>=4


@pytest.mark.parametrize('kind',['footnote','table','marginalia','caption','figure'])
def test_significant_unsupported_structure_is_explicit_failure(kind):
    with pytest.raises(SourceUnitPdfError,match='unsupported_structure:'+kind):
        render_source_unit_pdf(**inputs([(kind,'Точный важный авторский элемент')]))


@pytest.mark.parametrize('change',[
    dict(state='unit_not_ready'),dict(start_anchor=None),dict(end_anchor=None),dict(source_spans_has_more=True),
    dict(source_span_count=100),dict(source_spans=[]),
])
def test_incomplete_or_bounded_preview_never_returns_partial_pdf(change):
    values=inputs();values['unit'].update(change)
    with pytest.raises(SourceUnitPdfError,match='unit_not_ready'):
        render_source_unit_pdf(**values)


def test_sparse_metadata_never_fabricates_bibliography():
    values=inputs();values['edition']=StartMetadataInput(title='Подтверждённое название')
    with render(values) as doc:
        text=doc[0].get_text()
        assert 'Подтверждённое название' in text
        assert not any(x in text for x in ['ISBN','ISSN','DOI','УДК','ББК','1901','Автор /'])


def test_unsupported_glyph_never_silently_disappears():
    with pytest.raises(SourceUnitPdfError,match='render_text_fidelity_failed'):
        render_source_unit_pdf(**inputs([('body','До \U0010ffff после')]))


def test_bad_order_overlap_review_and_revision_are_rejected():
    for mode in ('order','overlap','review','revision'):
        values=inputs()
        if mode=='order':values['unit']['source_spans'].reverse()
        elif mode=='overlap':values['unit']['source_spans']*=2
        elif mode=='review':values['regions'][0].needs_review=True
        else:values['accepted_revision']=None
        with pytest.raises(SourceUnitPdfError):render_source_unit_pdf(**values)


def test_render_budgets_fail_closed(monkeypatch):
    import regional_knowledge.source_unit_pdf as module
    values=inputs()
    for name,value,error in [('MAX_CHARACTERS',1,'limit'),('MAX_PAGES',1,'page_limit'),
            ('MAX_BYTES',1,'byte_limit'),('MAX_SECONDS',-1,'deadline')]:
        with monkeypatch.context() as scoped:
            scoped.setattr(module,name,value)
            with pytest.raises(SourceUnitPdfError,match=error):render_source_unit_pdf(**values)


def test_pdf_bytes_are_deterministic_and_revision_is_bound_in_metadata():
    values=inputs([('body','office affine verified fi fl ffi ffl ﬁ ﬃ '+SAMPLE)])
    one=render_source_unit_pdf(**values)
    assert one==render_source_unit_pdf(**values)
    with pymupdf.open(stream=one,filetype='pdf') as doc:
        assert SOURCE in doc.metadata['keywords']
        assert 'accepted_revision:1' in doc.metadata['keywords']
        assert flatten(author_text(doc))==flatten(values['regions'][0].source_text)
    values['accepted_revision']=2
    assert one!=render_source_unit_pdf(**values)


def test_orphan_heading_and_overwide_unbreakable_text_fail_explicitly():
    with pytest.raises(SourceUnitPdfError,match='orphan_heading'):
        render_source_unit_pdf(**inputs([('heading','Unattached heading')]))
    with pytest.raises(SourceUnitPdfError,match='layout_bounds|fidelity|measurement'):
        render_source_unit_pdf(**inputs([('body','W'*500)]))


def test_terminal_anchor_must_match_last_nonempty_span_exactly():
    for mode in ('shortened','end_before_span','missing_terminal_region','wrong_page'):
        values=inputs()
        if mode=='shortened':values['unit']['source_spans'][-1]['end']-=1
        elif mode=='end_before_span':values['unit']['end_anchor']['offset']-=1
        elif mode=='missing_terminal_region':values['unit']['end_anchor']['region_id']='missing'
        else:values['unit']['end_anchor']['physical_page_index']=1
        with pytest.raises(SourceUnitPdfError,match='exact_source_end_required'):
            render_source_unit_pdf(**values)


def test_half_open_next_region_zero_offset_uses_accepted_endpoint_without_rendering_it():
    values=inputs([('body','Complete author text.'),('heading','Next unit is excluded.')])
    endpoint=values['unit']['source_spans'].pop()
    values['unit']['end_anchor']=dict(endpoint,offset=0)
    with render(values) as doc:
        assert flatten(author_text(doc))==flatten('Complete author text.')
        assert 'Next unit' not in author_text(doc)
    # Offset zero does not excuse truncating the preceding author region.
    values['unit']['source_spans'][-1]['end']-=1
    with pytest.raises(SourceUnitPdfError,match='exact_source_end_required'):
        render_source_unit_pdf(**values)


def test_partial_supplied_interior_region_and_broken_region_edges_are_rejected():
    for which,edge in ((0,'end'),(1,'start'),(1,'end'),(2,'start')):
        values=inputs([('body','First exact region.'),('body','Middle exact region.'),
                       ('body','Last exact region.')])
        span=values['unit']['source_spans'][which]
        span[edge]+=1 if edge=='start' else -1
        with pytest.raises(SourceUnitPdfError,match='contiguous_source_range_required'):
            render_source_unit_pdf(**values)
