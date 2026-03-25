from __future__ import annotations

from io import BytesIO
import re
from typing import Any, Optional

from pypdf import PdfReader
from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import CandidateProfile, ResumeDocument
from core.schemas import CandidateProfilePayload, LearningSummary, ResumeUploadResponse


KNOWN_TITLES = [
    "founding operations lead",
    "operations lead",
    "chief of staff",
    "bizops",
    "business operations",
    "deployment strategist",
    "implementation lead",
    "technical product manager",
    "product manager",
    "program manager",
]
KNOWN_LOCATIONS = ["san francisco", "new york", "remote", "bay area", "nyc"]
KNOWN_DOMAINS = ["ai", "developer tools", "infra", "saas", "fintech", "b2b", "healthtech"]
KNOWN_STAGES = ["seed", "series a", "series b", "early-stage", "startup", "growth"]
SENIORITY_KEYWORDS = {
    "intern": "entry",
    "new grad": "entry",
    "associate": "junior",
    "manager": "mid",
    "lead": "senior",
    "head": "senior",
    "director": "staff",
    "chief": "executive",
    "founder": "executive",
    "vp": "executive",
}
PROFILE_SCHEMA_KEY = "structured_profile"
NETWORK_IMPORT_KEY = "network_import"
PROFILE_MODEL_FIELDS = {
    column.name
    for column in CandidateProfile.__table__.columns
    if column.name not in {"id", "created_at", "updated_at"}
}
PROFILE_INVENTORY_CATEGORY_SPECS = [
    {
        "key": "resume_text",
        "label": "Resume text",
        "storage_fields": ["raw_resume_text"],
        "provenance": "Uploaded or pasted by the operator",
        "usage": "Used for local resume parsing, profile review, and deterministic lead matching.",
        "processing_path": "local_only",
    },
    {
        "key": "profile_preferences",
        "label": "Profile preferences",
        "storage_fields": [
            "name",
            "preferred_titles_json",
            "core_titles_json",
            "adjacent_titles_json",
            "excluded_titles_json",
            "preferred_domains_json",
            "preferred_locations_json",
            "excluded_companies_json",
            "confirmed_skills_json",
            "competencies_json",
            "explicit_preferences_json",
            "stage_preferences_json",
            "stretch_role_families_json",
            "excluded_keywords_json",
            "seniority_guess",
            "min_seniority_band",
            "max_seniority_band",
            "minimum_fit_threshold",
        ],
        "provenance": "Derived from the saved candidate profile and operator edits.",
        "usage": "Used to filter, rank, and explain leads in the local workbench.",
        "processing_path": "cloud_assisted",
    },
    {
        "key": "structured_profile",
        "label": "Structured profile schema",
        "storage_fields": ["extracted_summary_json.structured_profile", "extracted_summary_json.profile_schema_version"],
        "provenance": "Generated from the local profile payload before persistence.",
        "usage": "Provides stable schema-backed targeting fields for discovery and scoring flows.",
        "processing_path": "cloud_assisted",
    },
    {
        "key": "network_contacts",
        "label": "Network contacts",
        "storage_fields": ["extracted_summary_json.network_import"],
        "provenance": "Imported locally from a CSV or pasted network export.",
        "usage": "Used only for local referral-path suggestions shown in the workbench.",
        "processing_path": "local_only",
    },
    {
        "key": "learning_state",
        "label": "Learning state",
        "storage_fields": ["extracted_summary_json.learning"],
        "provenance": "Derived from local feedback, application tracking, and discovery outcomes.",
        "usage": "Used to summarize learned boosts, penalties, and generated query context.",
        "processing_path": "cloud_assisted",
    },
]


def _match_known_terms(text: str, choices: list[str]) -> list[str]:
    lowered = text.lower()
    return sorted({choice for choice in choices if choice in lowered})


def _guess_seniority(text: str) -> str:
    lowered = text.lower()
    years = [int(item) for item in re.findall(r"(\d+)\+?\s+years", lowered)]
    if years:
        max_years = max(years)
        if max_years <= 2:
            return "junior"
        if max_years <= 5:
            return "mid"
        if max_years <= 8:
            return "senior"
        return "staff"

    for keyword, band in SENIORITY_KEYWORDS.items():
        if keyword in lowered:
            return band
    return "senior"


def _extract_summary(raw_text: str) -> dict:
    titles = _match_known_terms(raw_text, KNOWN_TITLES)
    locations = _match_known_terms(raw_text, KNOWN_LOCATIONS)
    domains = _match_known_terms(raw_text, KNOWN_DOMAINS)
    stages = _match_known_terms(raw_text, KNOWN_STAGES)
    seniority = _guess_seniority(raw_text)

    adjacent_titles = [title for title in ["business operations", "implementation lead", "program manager"] if title not in titles]
    preferred_titles = titles[:4] or ["chief of staff", "founding operations lead"]
    core_titles = preferred_titles[:2]
    stretch_families = ["go_to_market", "operations"] if "deployment strategist" in raw_text.lower() else ["operations"]

    return {
        "summary": f"Profile centered on {' / '.join(preferred_titles[:3])} with {seniority} seniority leaning.",
        "preferred_titles_json": preferred_titles,
        "adjacent_titles_json": adjacent_titles[:4],
        "core_titles_json": core_titles,
        "preferred_locations_json": locations or ["san francisco", "new york", "remote"],
        "preferred_domains_json": domains or ["ai", "developer tools", "infra"],
        "stage_preferences_json": stages or ["early-stage", "series a"],
        "seniority_guess": seniority,
        "min_seniority_band": "mid" if seniority in {"senior", "staff", "executive"} else "junior",
        "max_seniority_band": "staff" if seniority in {"staff", "executive"} else seniority,
        "stretch_role_families_json": stretch_families,
        "excluded_titles_json": ["intern", "account executive"],
        "excluded_keywords_json": ["phd required", "bar admission", "rocket propulsion"],
    }


def extract_text_from_resume_upload(filename: str, file_bytes: bytes) -> tuple[str, list[str]]:
    warnings: list[str] = []
    lowered = filename.lower()
    if lowered.endswith(".pdf"):
        reader = PdfReader(BytesIO(file_bytes))
        pages = [page.extract_text() or "" for page in reader.pages]
        text = "\n".join(page.strip() for page in pages if page.strip()).strip()
        if not text:
            raise ValueError("PDF text extraction returned no readable text.")
        warnings.append("PDF parsed locally with pypdf. Complex layouts may still need manual cleanup.")
        return text, warnings
    if lowered.endswith(".txt") or lowered.endswith(".md"):
        return file_bytes.decode("utf-8", errors="ignore"), warnings
    raise ValueError("Unsupported file type. Upload a PDF, TXT, or MD resume.")


def get_candidate_profile(session: Session) -> CandidateProfile:
    existing = session.scalar(select(CandidateProfile).order_by(CandidateProfile.id.asc()))
    if existing:
        return existing

    payload = CandidateProfilePayload(
        profile_schema_version="v1",
        name="Demo Candidate",
        preferred_titles_json=["chief of staff", "founding operations lead", "deployment strategist"],
        adjacent_titles_json=["business operations", "implementation lead", "technical product manager"],
        excluded_titles_json=["intern", "new grad", "account executive"],
        preferred_domains_json=["ai", "developer tools", "infra"],
        preferred_locations_json=["san francisco", "new york", "remote"],
        confirmed_skills_json=["stakeholder management", "sql", "cross-functional leadership"],
        competencies_json=["operator judgment", "process design", "zero-to-one execution"],
        explicit_preferences_json=["hands-on teams", "customer-facing work", "clear scope"],
        seniority_guess="senior",
        stage_preferences_json=["early-stage", "series a"],
        core_titles_json=["chief of staff", "founding operations lead"],
        excluded_keywords_json=["rocket propulsion", "phd required", "principal scientist"],
        min_seniority_band="mid",
        max_seniority_band="staff",
        stretch_role_families_json=["operations", "go_to_market"],
        minimum_fit_threshold=2.8,
        extracted_summary_json={"summary": "Default demo profile focused on early-stage operating roles."},
    )
    payload = _with_structured_profile(payload)
    profile = CandidateProfile(**_profile_model_values(payload))
    profile.extracted_summary_json = _merge_structured_profile(profile.extracted_summary_json or {}, payload)
    session.add(profile)
    session.flush()
    return profile


def profile_to_payload(profile: CandidateProfile) -> CandidateProfilePayload:
    extracted_summary = profile.extracted_summary_json or {}
    structured_profile = extracted_summary.get(PROFILE_SCHEMA_KEY)
    payload = CandidateProfilePayload(
        profile_schema_version=structured_profile.get("version", "v1") if structured_profile else "v1",
        name=profile.name,
        raw_resume_text=profile.raw_resume_text,
        extracted_summary_json=extracted_summary,
        preferred_titles_json=profile.preferred_titles_json or [],
        adjacent_titles_json=profile.adjacent_titles_json or [],
        excluded_titles_json=profile.excluded_titles_json or [],
        preferred_domains_json=profile.preferred_domains_json or [],
        excluded_companies_json=profile.excluded_companies_json or [],
        preferred_locations_json=profile.preferred_locations_json or [],
        seniority_guess=profile.seniority_guess,
        stage_preferences_json=profile.stage_preferences_json or [],
        core_titles_json=profile.core_titles_json or [],
        excluded_keywords_json=profile.excluded_keywords_json or [],
        min_seniority_band=profile.min_seniority_band,
        max_seniority_band=profile.max_seniority_band,
        stretch_role_families_json=profile.stretch_role_families_json or [],
        minimum_fit_threshold=profile.minimum_fit_threshold,
        structured_profile_json=structured_profile,
    )
    return _with_structured_profile(payload)


def update_candidate_profile(session: Session, payload: CandidateProfilePayload) -> CandidateProfile:
    payload = _with_structured_profile(payload)
    profile = get_candidate_profile(session)
    for key, value in _profile_model_values(payload).items():
        setattr(profile, key, value)
    profile.extracted_summary_json = _merge_structured_profile(profile.extracted_summary_json or {}, payload)
    session.flush()
    return profile


def ingest_resume(session: Session, filename: str, raw_text: str) -> ResumeUploadResponse:
    parsed = _extract_summary(raw_text)
    resume = ResumeDocument(filename=filename, raw_text=raw_text, parsed_json=parsed)
    session.add(resume)
    profile = get_candidate_profile(session)

    profile.raw_resume_text = raw_text
    profile.name = profile.name or filename.rsplit(".", 1)[0]
    profile.extracted_summary_json = {"summary": parsed["summary"], "resume_filename": filename}
    profile.preferred_titles_json = parsed["preferred_titles_json"]
    profile.adjacent_titles_json = parsed["adjacent_titles_json"]
    profile.excluded_titles_json = parsed["excluded_titles_json"]
    profile.preferred_domains_json = parsed["preferred_domains_json"]
    profile.preferred_locations_json = parsed["preferred_locations_json"]
    profile.seniority_guess = parsed["seniority_guess"]
    profile.stage_preferences_json = parsed["stage_preferences_json"]
    profile.core_titles_json = parsed["core_titles_json"]
    profile.excluded_keywords_json = parsed["excluded_keywords_json"]
    profile.min_seniority_band = parsed["min_seniority_band"]
    profile.max_seniority_band = parsed["max_seniority_band"]
    profile.stretch_role_families_json = parsed["stretch_role_families_json"]
    payload = _with_structured_profile(profile_to_payload(profile))
    profile.extracted_summary_json = _merge_structured_profile(
        {"summary": parsed["summary"], "resume_filename": filename},
        payload,
    )

    session.flush()
    return ResumeUploadResponse(
        resume_document_id=resume.id,
        candidate_profile=profile_to_payload(profile),
        warnings=[],
    )


def build_learning_summary(profile: CandidateProfile) -> LearningSummary:
    learning = profile.extracted_summary_json.get("learning", {}) if profile.extracted_summary_json else {}
    boosted_titles = [title for title, value in sorted(learning.get("title_weights", {}).items(), key=lambda item: item[1], reverse=True)[:4]]
    boosted_domains = [domain for domain, value in sorted(learning.get("domain_weights", {}).items(), key=lambda item: item[1], reverse=True)[:4]]
    penalized_sources = [source for source, value in sorted(learning.get("source_penalties", {}).items(), key=lambda item: item[1], reverse=True)[:3]]
    generated_queries = learning.get("generated_queries", [])[-5:]
    return LearningSummary(
        boosted_titles=boosted_titles,
        boosted_domains=boosted_domains,
        penalized_sources=penalized_sources,
        generated_queries=generated_queries,
    )


def extract_network_import(extracted_summary_json: Optional[dict]) -> dict:
    summary = extracted_summary_json or {}
    network_payload = summary.get(NETWORK_IMPORT_KEY)
    return network_payload if isinstance(network_payload, dict) else {}


def attach_network_import(extracted_summary_json: Optional[dict], network_payload: dict) -> dict:
    merged = dict(extracted_summary_json or {})
    merged[NETWORK_IMPORT_KEY] = network_payload
    return merged


def build_profile_data_inventory(profile: CandidateProfile | dict[str, Any]) -> list[dict[str, Any]]:
    profile_data = _profile_data_dict(profile)
    summary = profile_data.get("extracted_summary_json") or {}
    learning = summary.get("learning") if isinstance(summary.get("learning"), dict) else {}
    network_payload = extract_network_import(summary)
    inventory_rows: list[dict[str, Any]] = []

    for spec in PROFILE_INVENTORY_CATEGORY_SPECS:
        key = spec["key"]
        if key == "resume_text":
            item_count = 1 if (profile_data.get("raw_resume_text") or "").strip() else 0
            example_values = [summary.get("resume_filename") or "pasted resume"] if item_count else []
        elif key == "profile_preferences":
            item_count = sum(
                1
                for field_name in spec["storage_fields"]
                if _field_has_value(profile_data.get(field_name))
            )
            example_values = [
                *(profile_data.get("core_titles_json") or [])[:2],
                *(profile_data.get("preferred_domains_json") or [])[:2],
            ]
        elif key == "structured_profile":
            structured_profile = summary.get(PROFILE_SCHEMA_KEY) if isinstance(summary.get(PROFILE_SCHEMA_KEY), dict) else {}
            item_count = len(structured_profile)
            example_values = list(structured_profile.keys())[:3]
        elif key == "network_contacts":
            contacts = network_payload.get("contacts") or []
            item_count = len(contacts)
            example_values = [contact.get("company") or contact.get("name") or "" for contact in contacts[:3]]
        else:
            item_count = sum(len(value) if isinstance(value, list) else 1 for value in learning.values()) if learning else 0
            example_values = [
                *(learning.get("generated_queries") or [])[:2],
                *list((learning.get("title_weights") or {}).keys())[:2],
            ]

        inventory_rows.append(
            {
                "category_key": key,
                "category": spec["label"],
                "stored": item_count > 0,
                "item_count": item_count,
                "storage_fields": spec["storage_fields"],
                "provenance": spec["provenance"],
                "usage": spec["usage"],
                "processing_path": spec["processing_path"],
                "example_values": [value for value in example_values if value],
            }
        )
    return inventory_rows


def _with_structured_profile(payload: CandidateProfilePayload) -> CandidateProfilePayload:
    return CandidateProfilePayload(**payload.model_dump())


def _merge_structured_profile(extracted_summary_json: dict, payload: CandidateProfilePayload) -> dict:
    merged = dict(extracted_summary_json)
    merged["profile_schema_version"] = payload.profile_schema_version
    merged[PROFILE_SCHEMA_KEY] = payload.structured_profile_json.model_dump() if payload.structured_profile_json else {}
    return merged


def _profile_data_dict(profile: CandidateProfile | dict[str, Any]) -> dict[str, Any]:
    if isinstance(profile, dict):
        return dict(profile)
    return {field_name: getattr(profile, field_name, None) for field_name in PROFILE_MODEL_FIELDS | {"name", "extracted_summary_json"}}


def _field_has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _profile_model_values(payload: CandidateProfilePayload) -> dict:
    return payload.model_dump(include=PROFILE_MODEL_FIELDS)
