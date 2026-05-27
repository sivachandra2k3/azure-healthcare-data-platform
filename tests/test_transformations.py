"""
tests/test_transformations.py
Unit tests for healthcare data transformation logic.
Run with: pytest tests/ -v
"""

import pytest
from datetime import date, datetime
from unittest.mock import MagicMock, patch


# ── Helpers (replicate key logic without Spark for unit testing) ─

def standardize_gender(raw: str) -> str:
    if raw is None:
        return "Unknown"
    val = raw.strip().upper()
    if val in ("M", "MALE"):
        return "Male"
    elif val in ("F", "FEMALE"):
        return "Female"
    return "Unknown"


def calculate_age(date_of_birth: date, reference_date: date = None) -> int:
    ref = reference_date or date.today()
    age = ref.year - date_of_birth.year
    if (ref.month, ref.day) < (date_of_birth.month, date_of_birth.day):
        age -= 1
    return age


def get_age_group(age: int) -> str:
    if age < 18:  return "Pediatric"
    if age < 40:  return "Young Adult"
    if age < 60:  return "Middle Aged"
    if age < 80:  return "Senior"
    return "Elderly"


def calculate_risk_score(age: int, los: int, readmission: bool, age_group: str) -> float:
    score = age * 0.3 + los * 2.5
    if readmission:  score += 20
    if age_group == "Elderly": score += 10
    if age_group == "Senior":  score += 5
    return min(100.0, round(score, 1))


def get_risk_category(risk_score: float) -> str:
    if risk_score >= 70: return "High Risk"
    if risk_score >= 40: return "Medium Risk"
    return "Low Risk"


def calculate_coverage_ratio(insurance_covered: float, total_amount: float) -> float:
    if total_amount == 0:
        return 0.0
    return round(insurance_covered / total_amount, 4)


def validate_icd10_code(code: str) -> bool:
    """Basic ICD-10 format check: letter + 2 digits + optional decimal."""
    import re
    if not code:
        return False
    pattern = r"^[A-Z]\d{2}(\.\d{1,4})?$"
    return bool(re.match(pattern, code.upper().strip()))


# ── Tests: Gender Standardization ──────────────────────────────

class TestGenderStandardization:
    def test_male_abbreviation(self):
        assert standardize_gender("M") == "Male"

    def test_female_abbreviation(self):
        assert standardize_gender("F") == "Female"

    def test_male_full(self):
        assert standardize_gender("male") == "Male"

    def test_female_full(self):
        assert standardize_gender("FEMALE") == "Female"

    def test_lowercase_m(self):
        assert standardize_gender("m") == "Male"

    def test_unknown_value(self):
        assert standardize_gender("Other") == "Unknown"

    def test_none_value(self):
        assert standardize_gender(None) == "Unknown"

    def test_whitespace(self):
        assert standardize_gender("  F  ") == "Female"


# ── Tests: Age Calculation ──────────────────────────────────────

class TestAgeCalculation:
    def test_basic_age(self):
        dob = date(1980, 1, 1)
        ref = date(2024, 1, 1)
        assert calculate_age(dob, ref) == 44

    def test_birthday_not_yet_this_year(self):
        dob = date(1990, 12, 31)
        ref = date(2024, 6, 15)
        assert calculate_age(dob, ref) == 33

    def test_birthday_today(self):
        dob = date(1990, 6, 15)
        ref = date(2024, 6, 15)
        assert calculate_age(dob, ref) == 34

    def test_newborn(self):
        dob = date(2024, 1, 1)
        ref = date(2024, 6, 1)
        assert calculate_age(dob, ref) == 0


# ── Tests: Age Group ────────────────────────────────────────────

class TestAgeGroup:
    def test_pediatric(self):
        assert get_age_group(10) == "Pediatric"

    def test_young_adult(self):
        assert get_age_group(25) == "Young Adult"

    def test_middle_aged(self):
        assert get_age_group(50) == "Middle Aged"

    def test_senior(self):
        assert get_age_group(70) == "Senior"

    def test_elderly(self):
        assert get_age_group(85) == "Elderly"

    def test_boundary_18(self):
        assert get_age_group(18) == "Young Adult"

    def test_boundary_80(self):
        assert get_age_group(80) == "Elderly"


# ── Tests: Risk Score ───────────────────────────────────────────

class TestRiskScore:
    def test_low_risk_young(self):
        score = calculate_risk_score(age=25, los=2, readmission=False, age_group="Young Adult")
        assert score < 40

    def test_high_risk_elderly_readmitted(self):
        score = calculate_risk_score(age=82, los=14, readmission=True, age_group="Elderly")
        assert score == 100.0  # capped at 100

    def test_readmission_adds_20(self):
        s1 = calculate_risk_score(40, 5, False, "Middle Aged")
        s2 = calculate_risk_score(40, 5, True,  "Middle Aged")
        assert s2 - s1 == 20.0

    def test_elderly_bonus_10(self):
        s1 = calculate_risk_score(80, 5, False, "Senior")
        s2 = calculate_risk_score(80, 5, False, "Elderly")
        assert s2 - s1 == 5.0

    def test_capped_at_100(self):
        score = calculate_risk_score(200, 100, True, "Elderly")
        assert score == 100.0


# ── Tests: Risk Category ────────────────────────────────────────

class TestRiskCategory:
    def test_high_risk(self):
        assert get_risk_category(75.0) == "High Risk"

    def test_medium_risk(self):
        assert get_risk_category(55.0) == "Medium Risk"

    def test_low_risk(self):
        assert get_risk_category(20.0) == "Low Risk"

    def test_boundary_70(self):
        assert get_risk_category(70.0) == "High Risk"

    def test_boundary_40(self):
        assert get_risk_category(40.0) == "Medium Risk"

    def test_boundary_39(self):
        assert get_risk_category(39.9) == "Low Risk"


# ── Tests: Coverage Ratio ───────────────────────────────────────

class TestCoverageRatio:
    def test_full_coverage(self):
        assert calculate_coverage_ratio(100000, 100000) == 1.0

    def test_90_percent_coverage(self):
        assert calculate_coverage_ratio(90000, 100000) == 0.9

    def test_zero_total(self):
        assert calculate_coverage_ratio(0, 0) == 0.0

    def test_partial_coverage(self):
        ratio = calculate_coverage_ratio(256500, 285000)
        assert abs(ratio - 0.9) < 0.01


# ── Tests: ICD-10 Validation ────────────────────────────────────

class TestICD10Validation:
    def test_valid_code_simple(self):
        assert validate_icd10_code("E11") is True

    def test_valid_code_with_decimal(self):
        assert validate_icd10_code("E11.9") is True

    def test_valid_code_long(self):
        assert validate_icd10_code("I25.10") is True

    def test_invalid_starts_with_number(self):
        assert validate_icd10_code("111.9") is False

    def test_invalid_empty(self):
        assert validate_icd10_code("") is False

    def test_invalid_none(self):
        assert validate_icd10_code(None) is False

    def test_lowercase_input(self):
        assert validate_icd10_code("e11.9") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
