import pytest
from pydantic import ValidationError

from regional_knowledge.contracts import BBox, RightsStatus, Visibility
from regional_knowledge.rights import (
    RightsFacts,
    assess_public_domain_candidate,
    assert_visibility_allowed,
)


def test_bbox_is_named_normalized_and_positive_area():
    assert BBox(left=0, top=1, right=999, bottom=1000).right == 999
    with pytest.raises(ValidationError):
        BBox(left=500, top=1, right=500, bottom=900)


def test_unverified_rights_cannot_be_public():
    with pytest.raises(ValueError):
        assert_visibility_allowed(Visibility.PUBLIC, RightsStatus.PUBLIC_DOMAIN_CANDIDATE)
    assert_visibility_allowed(Visibility.PUBLIC, RightsStatus.PUBLIC_DOMAIN_VERIFIED)


def test_old_publication_is_not_enough_for_public_domain():
    result = assess_public_domain_candidate(
        RightsFacts(
            jurisdiction="DE",
            publication_year=1930,
            author_identified=False,
            special_case_review_complete=False,
        ),
        current_year=2026,
    )
    assert result.status is RightsStatus.UNKNOWN


def test_russian_term_is_only_candidate_not_verified():
    result = assess_public_domain_candidate(
        RightsFacts(
            jurisdiction="RU",
            author_identified=True,
            author_death_year=1940,
            publication_year=1930,
            wwii_work_or_participation=False,
            repression_or_rehabilitation_case=False,
            posthumous_first_publication_case=False,
            special_case_review_complete=True,
        ),
        current_year=2026,
    )
    assert result.status is RightsStatus.PUBLIC_DOMAIN_CANDIDATE
