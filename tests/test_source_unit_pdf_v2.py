"""PDF v2 distinguishing test: optional client-authored Russian text, never Kaggle LLM."""
import re

import pytest

pymupdf = pytest.importorskip('pymupdf')

from regional_knowledge.source_unit_pdf import render_source_unit_pdf
from test_source_unit_pdf import inputs, flatten


def page_body(page):
    return ''.join(block[4] for block in page.get_text('blocks', sort=False)
        if block[6] == 0 and 49 <= block[1] < page.rect.height - 43)


def check_furniture(doc, unit, edition):
    running = ' · '.join((unit['title'], '; '.join(edition.authors), edition.title))
    for number, page in enumerate(doc, 1):
        blocks = page.get_text('blocks', sort=False)
        header = ''.join(b[4] for b in blocks if b[6] == 0 and b[3] < 49)
        footer = ''.join(b[4] for b in blocks if b[6] == 0 and b[1] > page.rect.height - 43)
        assert flatten(header) == flatten(running)
        assert flatten(footer) == str(number)
        assert 'Страница PDF' not in page.get_text()


def test_pdf_v2_client_annotation_and_full_translation_follow_exact_author_text():
    author = 'Hoffmann, Ernst Theodor Amadeus, schrieb über Königsberg.\nZweiter Satz mit ß und ѣ.'
    values = inputs([('body', author)])
    values['unit'].update(kind='article', title='Hoffmann, Ernst Theodor Amadeus')
    annotation = 'Писатель и композитор, связанный с Кёнигсбергом.\n\nСтатья рассказывает о его деятельности.'
    translation = 'Гофман, Эрнст Теодор Амадей, писал о Кёнигсберге.\nВторое предложение.'
    with pymupdf.open(stream=render_source_unit_pdf(
            **values, annotation_ru=annotation, translation_ru=translation), filetype='pdf') as doc:
        assert len(doc) >= 2
        check_furniture(doc, values['unit'], values['edition'])
        cover = page_body(doc[0])
        assert cover.index('Hoffmann, Ernst') < cover.index('Проверенное издание')
        assert 'Краткое изложение, подготовлено сервисом' in cover
        assert flatten(annotation) in flatten(cover)
        assert 'verified-isbn' in cover and 'verified-doi' in cover
        assert max(span['size'] for block in doc[0].get_text('dict')['blocks']
            for line in block.get('lines', []) for span in line['spans']) >= 19
        authored = ''.join(page_body(page) for page in list(doc)[1:])
        expected = values['unit']['title'] + author + 'Автоматический перевод; авторитетен оригинал выше' + translation
        assert flatten(authored) == flatten(expected)
        assert flatten(author) in flatten(authored)
        assert authored.index('Автоматический перевод') > authored.index('Zweiter Satz')
        assert 'Краткое изложение' not in authored


def test_pdf_v2_without_optional_fields_has_no_empty_sections():
    values = inputs([('body', 'Первоисточник дословно без перевода.')])
    with pymupdf.open(stream=render_source_unit_pdf(**values), filetype='pdf') as doc:
        assert len(doc) == 2
        check_furniture(doc, values['unit'], values['edition'])
        assert 'Точная глава' in page_body(doc[0])
        assert flatten(page_body(doc[1])) == flatten('Точная глава' + values['regions'][0].source_text)
        entire = ''.join(p.get_text() for p in doc)
        assert 'Автоматический перевод' not in entire
        assert 'Краткое изложение' not in entire
        assert re.search(r'Точная глава', entire)
