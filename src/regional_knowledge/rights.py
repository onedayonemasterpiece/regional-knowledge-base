from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from .contracts import PUBLIC_RIGHTS, RightsStatus, Visibility


@dataclass(frozen=True)
class RightsFacts:
    jurisdiction: str | None = None
    author_identified: bool | None = None
    author_death_year: int | None = None
    publication_year: int | None = None
    anonymous_or_pseudonymous: bool | None = None
    wwii_work_or_participation: bool | None = None
    repression_or_rehabilitation_case: bool | None = None
    posthumous_first_publication_case: bool | None = None
    special_case_review_complete: bool = False


@dataclass(frozen=True)
class RightsAssessment:
    status: RightsStatus
    rationale: str


def can_be_public(status: RightsStatus) -> bool:
    """Database/application invariant for public content visibility."""

    return status in PUBLIC_RIGHTS


def assert_visibility_allowed(visibility: Visibility, status: RightsStatus) -> None:
    if visibility is Visibility.PUBLIC and not can_be_public(status):
        raise ValueError("public visibility requires a verified rights basis")


def assess_public_domain_candidate(
    facts: RightsFacts, *, current_year: int | None = None
) -> RightsAssessment:
    """Conservative triage only; never returns PUBLIC_DOMAIN_VERIFIED.

    The function is deliberately unable to make a legal/publication decision. It
    helps prioritize records for evidence review and fails closed on incomplete
    special-case facts.
    """

    year = current_year or date.today().year
    jurisdiction = (facts.jurisdiction or "").upper()

    if not facts.special_case_review_complete:
        return RightsAssessment(
            RightsStatus.UNKNOWN,
            "special-case rights review is incomplete",
        )

    if facts.author_identified and facts.author_death_year:
        extra = 0
        if jurisdiction in {"RU", "RUS", "RUSSIA"}:
            if facts.wwii_work_or_participation is None:
                return RightsAssessment(
                    RightsStatus.UNKNOWN,
                    "Russian WWII-term fact is unknown",
                )
            extra = 4 if facts.wwii_work_or_participation else 0
            if facts.repression_or_rehabilitation_case is None:
                return RightsAssessment(
                    RightsStatus.UNKNOWN,
                    "Russian rehabilitation special-case fact is unknown",
                )
            if facts.posthumous_first_publication_case is None:
                return RightsAssessment(
                    RightsStatus.UNKNOWN,
                    "Russian posthumous-publication fact is unknown",
                )
            if facts.repression_or_rehabilitation_case or facts.posthumous_first_publication_case:
                return RightsAssessment(
                    RightsStatus.UNKNOWN,
                    "special statutory term requires case-specific verification",
                )

        if jurisdiction in {"RU", "RUS", "RUSSIA", "DE", "DEU", "GERMANY"}:
            expiry_boundary = facts.author_death_year + 70 + extra
            if year > expiry_boundary:
                return RightsAssessment(
                    RightsStatus.PUBLIC_DOMAIN_CANDIDATE,
                    "age/death-term facts support manual or evidence-backed verification",
                )

    return RightsAssessment(
        RightsStatus.UNKNOWN,
        "available facts do not establish a public-domain candidate",
    )
