#!/usr/bin/env python3

from __future__ import annotations

import re
import unicodedata


DEPARTMENT_CODE_TO_NAME: dict[str, str] = {
    "mahagri": "Agriculture, Dairy Development, Animal Husbandry and Fisheries Department",
    "mahcoop": "Co-operation, Textiles and Marketing Department",
    "mahenv": "Environment Department",
    "mahfin": "Finance Department",
    "mahfood": "Food, Civil Supplies and Consumer Protection Department",
    "mahadmin": "General Administration Department",
    "mahtech": "Higher and Technical Education Department",
    "mahhome": "Home Department",
    "mahhouse": "Housing Department",
    "mahind": "Industries, Energy and Labour Department",
    "mahit": "Information Technology Department",
    "mahlaw": "Law and Judiciary Department",
    "mahmar": "Marathi Language Department",
    "mahmed": "Medical Education and Drugs Department",
    "mahmin": "Minorities Development Department",
    "mahbah": "Other Backward Bahujan Welfare Department",
    "mahpar": "Parliamentary Affairs Department",
    "mahdis": "Persons with Disabilities Welfare Department",
    "mahplan": "Planning Department",
    "mahhea": "Public Health Department",
    "mahpwd": "Public Works Department",
    "mahrev": "Revenue and Forest Department",
    "mahrural": "Rural Development Department",
    "mahedu": "School Education and Sports Department",
    "mahskill": "Skill Development and Entrepreneurship Department",
    "mahsoc": "Social Justice and Special Assistance Department",
    "mahsoil": "Soil and Water Conservation Department",
    "mahtour": "Tourism and Cultural Affairs Department",
    "mahtrib": "Tribal Development Department",
    "mahurb": "Urban Development Department",
    "mahwater": "Water Resources Department",
    "mahsanit": "Water Supply and Sanitation Department",
    "mahwom": "Women and Child Development Department",
}


def _ascii_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return normalized


def _normalize_name(value: str) -> str:
    text = _ascii_text(value).replace("&", " and ").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _fallback_slug(value: str) -> str:
    text = _ascii_text(value).replace("&", " and ").lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "unknown"


_NAME_TO_CODE = {
    _normalize_name(department_name): department_code
    for department_code, department_name in DEPARTMENT_CODE_TO_NAME.items()
}


def department_code_from_name(department_name: str) -> str:
    text = (department_name or "").strip()
    if not text:
        return "unknown"

    as_code = text.lower()
    if as_code in DEPARTMENT_CODE_TO_NAME:
        return as_code

    normalized_name = _normalize_name(text)
    if normalized_name in _NAME_TO_CODE:
        return _NAME_TO_CODE[normalized_name]

    return _fallback_slug(text)
