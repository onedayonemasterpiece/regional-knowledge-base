"""Journal durability and authority across two independent research accounts."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from regional_knowledge.contracts import Principal
from regional_knowledge.disagreement_contracts import (
    CaseCreate, CaseGrant, CaseRevise, CaseState, ConclusionAdd,
    ContributionAdd, ContributionCorrect, EvidenceAdd,
)
from regional_knowledge.disagreement_journal import DisagreementJournal, JournalError
from regional_knowledge.sqlite_corpus import SQLiteCorpus


@pytest.fixture
def journal(tmp_path):
    corpus = SQLiteCorpus(tmp_path / "corpus.sqlite3")
    actors = [Principal(subject=str(uuid4()), client_id="test", issuer="test", access_token="synthetic") for _ in range(3)]
    corpus.put("rkb_users", [{"id": a.subject, "status": "active"} for a in actors])
    store = DisagreementJournal(corpus)
    created = store.create(actors[0], CaseCreate(question="К какому событию относится дата?",
        context="Синтетическое исследование двух событий", classification="candidate",
        rationale="Разобрать событие и говорящего",
        positions=[{"id": "a", "text": "Открытие в 1888", "speaker": "Автор А", "modality": "asserted"},
                   {"id": "b", "text": "Окончание в 1894", "speaker": "Автор Б", "modality": "asserted"}]), "create-test-case")
    return SimpleNamespace(corpus=corpus, store=store, owner=actors[0], other=actors[1], outsider=actors[2], case=created["case_id"], created=created)


def grant(j, actor, role="contributor"):
    return j.store.grant(j.owner, j.case, j.store.get(j.owner, j.case)["case_revision"],
                         CaseGrant(grantee=actor.subject, capability=role, reason="Synthetic grant"), "grant-" + actor.subject)


def expect(code, fn, *args, **kwargs):
    with pytest.raises(JournalError) as error:
        fn(*args, **kwargs)
    assert error.value.code == code


def evidence(j, actor=None):
    actor = actor or j.owner
    doc, page, region = [str(uuid4()) for _ in range(3)]
    text = "Синтетический источник. Здание открыто в 1888 году."
    j.corpus.put("rkb_documents", [{"id": doc, "owner_user_id": actor.subject,
        "source_sha256": "a" * 64, "active_revision": 1, "content_visibility": "private"}])
    j.corpus.put("rkb_pages", [{"id": page, "document_id": doc, "revision": 1, "physical_page_index": 4}])
    j.corpus.put("rkb_regions", [{"id": region, "page_id": page, "source_text": text}])
    return EvidenceAdd(document_id=doc, source_revision=1, source_sha256="a" * 64,
        page_id=page, region_id=region, text_start=text.index("Здание"), quote="Здание открыто в 1888 году.",
        context=text, position_ids=["a"], relation="supports", speaker="Автор А", attribution="direct_statement")


def test_create_status_survives_restart_without_known_case_id(journal):
    j=journal
    reopened=DisagreementJournal(j.corpus)
    assert reopened.operation_status(j.owner,"create-test-case")==j.created
    assert reopened.operation_status(j.other,"create-test-case")=={"state":"not_found"}
    with j.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM disagreement_cases").fetchone()[0]==1


def test_concurrent_independent_contributions_and_same_key_replay(journal):
    j=journal;grant(j,j.other)
    payload=ContributionAdd(kind="opinion",original_text="Проверить разные события", interpretation="Модельная интерпретация")
    def save(actor,key):return DisagreementJournal(j.corpus).contribute(actor,j.case,payload,key)
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures=[pool.submit(save,j.owner,"owner-opinion"),pool.submit(save,j.other,"expert-opinion"),pool.submit(save,j.other,"expert-opinion")]
        results=[f.result() for f in futures]
    assert results[1]==results[2]
    rows=j.store.get(j.owner,j.case,section="contributions")["items"]
    assert len(rows)==2 and {r["actor_id"] for r in rows}=={j.owner.subject,j.other.subject}
    assert not any(r["payload"]["human_reading_confirmed"] for r in rows)
    expect("idempotency_conflict",j.store.contribute,j.other,j.case,ContributionAdd(kind="opinion",original_text="changed"),"expert-opinion")


def test_case_grant_does_not_grant_private_source_or_receipt(journal):
    j=journal;grant(j,j.other)
    cmd=evidence(j)
    j.store.add_evidence(j.owner,j.case,cmd,"source-evidence")
    expect("not_found_or_not_accessible",j.store.get,j.other,j.case)
    # Explicit source-text grant permits the case, not private original bytes.
    j.corpus.put("rkb_document_grants",[{"document_id":str(cmd.document_id),"grantee_user_id":j.other.subject}])
    saved=j.store.contribute(j.other,j.case,ContributionAdd(kind="opinion",original_text="Сравнил текст"),"review-source")
    with j.corpus.connect() as db:
        db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants' AND json_extract(payload,'$.document_id')=? AND json_extract(payload,'$.grantee_user_id')=?", (str(cmd.document_id),j.other.subject))
    expect("not_found_or_not_accessible",j.store.operation_status,j.other,"review-source")
    expect("not_found_or_not_accessible",j.store.get,j.other,j.case,section="history")
    assert saved["state"]=="applied"


def test_exact_quote_source_revision_and_no_invented_visual_confirmation(journal):
    j=journal;cmd=evidence(j)
    for change in [{"source_revision":2},{"source_sha256":"b"*64},{"text_start":0},{"position_ids":["unknown"]}]:
        expect("invalid_position" if "position_ids" in change else "quote_mismatch" if "text_start" in change else "invalid_evidence",
               j.store.add_evidence,j.owner,j.case,cmd.model_copy(update=change),"wrong-"+next(iter(change)))
    saved=j.store.add_evidence(j.owner,j.case,cmd,"valid-evidence")
    row=j.store.get(j.owner,j.case,section="evidence")["items"][0]
    assert row["id"]==saved["evidence_id"] and row["payload"]["verification"]=="accepted_text_match"
    assert row["payload"]["visual_confirmation"] is None


def test_owner_cannot_rewrite_another_contribution_and_versions_are_retained(journal):
    j=journal;grant(j,j.other)
    op=j.store.contribute(j.other,j.case,ContributionAdd(kind="opinion",original_text="Первое мнение"),"first-contribution")
    correction=ContributionCorrect(original_text="Уточнённое мнение",reason="Нашёл контекст",withdrawn=True)
    expect("not_found_or_not_accessible",j.store.correct_contribution,j.owner,j.case,op["contribution_id"],1,correction,"owner-rewrite")
    second=j.store.correct_contribution(j.other,j.case,op["contribution_id"],1,correction,"self-corrected")
    expect("revision_conflict",j.store.correct_contribution,j.other,j.case,op["contribution_id"],1,correction,"stale-correction")
    rows=j.store.get(j.owner,j.case,section="contributions")["items"]
    assert [r["version"] for r in rows]==[1,2] and second["version"]==2
    assert rows[0]["payload"]["original_text"]=="Первое мнение"


def test_precise_conclusion_snapshot_does_not_consume_late_argument(journal):
    j=journal;grant(j,j.other)
    old=j.store.contribute(j.other,j.case,ContributionAdd(kind="opinion",original_text="Первая позиция"),"initial-opinion")
    current=j.store.get(j.owner,j.case)
    j.store.contribute(j.other,j.case,ContributionAdd(kind="argument",original_text="Поздний аргумент"),"later-argument")
    cmd=ConclusionAdd(kind="model_synthesis",text="Предварительная сводка",outcome="insufficient_material",
        allowed_wording="Материалов недостаточно",limitations="Поздний аргумент ещё не рассмотрен",
        open_questions=["Прочитать оригинал"],based_on_seq=old["event_seq"],
        contribution_versions={old["contribution_id"]:1},weight_analysis="Свидетельств недостаточно независимо от статуса участников",make_current=True)
    j.store.conclude(j.owner,j.case,current["case_revision"],cmd,"new-conclusion")
    got=j.store.get(j.owner,j.case)
    assert got["new_material_since_conclusion"]
    assert got["current_conclusion"]["payload"]["human_support"]==[]
    assert got["current_conclusion"]["payload"]["based_on_seq"]==old["event_seq"]


def test_shared_cas_history_and_cursor_never_drop_older_content(journal):
    j=journal
    cmd=CaseRevise(question="Уточнённый вопрос",context="Контекст",classification="candidate",rationale="Различить события",reason="Уточнение")
    j.store.revise(j.owner,j.case,1,cmd,"revise-question")
    expect("revision_conflict",j.store.revise,j.owner,j.case,1,cmd,"stale-question")
    j.store.revise(j.owner,j.case,2,CaseState(state="paused",reason="Нужен оригинал"),"pause-research")
    assert j.store.get(j.owner,j.case,revision=1)["question"]=="К какому событию относится дата?"
    assert j.store.get(j.owner,j.case)["state"]=="paused"
    seen=[];cursor=0
    while True:
        page=j.store.get(j.owner,j.case,section="history",after_seq=cursor,limit=1)
        seen+=page["items"]
        if not page["has_more"]:break
        cursor=page["next_after_seq"]
    assert [r["seq"] for r in seen]==[1,2,3]


def test_failed_mutation_rolls_back_event_and_receipt_atomically(journal,monkeypatch):
    j=journal;old=j.store._event
    def broken(*args,**kwargs):
        old(*args,**kwargs)
        raise RuntimeError("crash before commit")
    monkeypatch.setattr(j.store,"_event",broken)
    cmd=ContributionAdd(kind="opinion",original_text="Сохраняемая позиция")
    with pytest.raises(RuntimeError,match="crash"):
        j.store.contribute(j.owner,j.case,cmd,"crash-contribution")
    assert j.store.operation_status(j.owner,"crash-contribution")=={"state":"not_found"}
    assert j.store.get(j.owner,j.case)["event_seq"]==1
    monkeypatch.setattr(j.store,"_event",old)
    saved=j.store.contribute(j.owner,j.case,cmd,"crash-contribution")
    assert saved["event_seq"]==2


def test_unknown_fields_cannot_forge_actor_or_human_confirmation():
    for extra in [{"actor_id":str(uuid4())},{"human_reading_confirmed":True},{"expert":True}]:
        with pytest.raises(ValidationError):
            ContributionAdd(kind="argument",original_text="Слова",**extra)


def test_stale_access_change_cannot_regrant_after_current_revocation(journal):
    j=journal
    granted=grant(j,j.other)
    j.store.grant(j.owner,j.case,granted["case_revision"],
        CaseGrant(grantee=j.other.subject,capability=None,reason="Отзыв"),"revoke-expert")
    expect("revision_conflict",j.store.grant,j.owner,j.case,granted["case_revision"],
        CaseGrant(grantee=j.other.subject,capability="manager",reason="Старый запрос"),"stale-regrant")
    expect("not_found_or_not_accessible",j.store.get,j.other,j.case)


def test_consistent_backup_preserves_contributions_versions_and_lost_reply_receipt(journal,tmp_path):
    j=journal;grant(j,j.other)
    saved=j.store.contribute(j.other,j.case,ContributionAdd(kind="argument",original_text="Невоспроизводимый вклад специалиста"),"durable-human-opinion")
    backup=tmp_path/"restore.sqlite3"
    with j.corpus.connect() as source, sqlite3.connect(backup) as target:
        source.backup(target)
    restored=DisagreementJournal(SQLiteCorpus(backup))
    assert restored.operation_status(j.other,"durable-human-opinion")==saved
    rows=restored.get(j.owner,j.case,section="contributions")["items"]
    assert rows[0]["actor_id"]==j.other.subject
    assert rows[0]["payload"]["original_text"]=="Невоспроизводимый вклад специалиста"
    j.corpus.put("rkb_users",[{"id":j.other.subject,"status":"inactive"}])
    expect("capability_denied",j.store.operation_status,j.other,"durable-human-opinion")
    assert restored.operation_status(j.other,"durable-human-opinion")==saved
