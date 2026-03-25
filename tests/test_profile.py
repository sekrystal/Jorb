from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base
from core.schemas import CandidateProfilePayload, StructuredCandidateProfile
from services.document_ingest import preview_resume_text, preview_resume_upload
from services.network_import import match_referral_paths, parse_network_csv
from services.profile import (
    attach_network_import,
    build_profile_data_inventory,
    extract_network_import,
    extract_text_from_resume_upload,
    get_candidate_profile,
    ingest_resume,
    profile_to_payload,
    update_candidate_profile,
)


def test_resume_ingestion_populates_candidate_profile() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    result = ingest_resume(
        session,
        filename="resume.txt",
        raw_text=(
            "Senior operator with 7+ years in AI and developer tools. "
            "Worked as chief of staff and deployment lead in San Francisco and New York."
        ),
    )

    profile = get_candidate_profile(session)
    assert result.resume_document_id is not None
    assert "chief of staff" in profile.preferred_titles_json
    assert profile.seniority_guess in {"senior", "staff"}
    assert profile.extracted_summary_json["profile_schema_version"] == "v1"
    assert profile.extracted_summary_json["structured_profile"]["targeting"]["preferred_titles"]


def test_candidate_profile_payload_builds_structured_schema_from_flat_fields() -> None:
    payload = CandidateProfilePayload(
        preferred_titles_json=["chief of staff", "operator"],
        core_titles_json=["chief of staff"],
        preferred_domains_json=["ai"],
        preferred_locations_json=["remote"],
        excluded_companies_json=["BigCo"],
        confirmed_skills_json=["sql", "stakeholder management"],
        competencies_json=["process design"],
        explicit_preferences_json=["hands-on teams"],
        stage_preferences_json=["series a"],
        stretch_role_families_json=["operations"],
        excluded_keywords_json=["phd required"],
        seniority_guess="senior",
        min_seniority_band="mid",
        max_seniority_band="staff",
        minimum_fit_threshold=3.2,
    )

    assert payload.profile_schema_version == "v1"
    assert payload.structured_profile_json is not None
    assert payload.structured_profile_json.targeting.preferred_titles == ["chief of staff", "operator"]
    assert payload.structured_profile_json.targeting.confirmed_skills == ["sql", "stakeholder management"]
    assert payload.structured_profile_json.targeting.competencies == ["process design"]
    assert payload.structured_profile_json.targeting.explicit_preferences == ["hands-on teams"]
    assert payload.structured_profile_json.targeting.seniority.maximum_band == "staff"
    assert payload.structured_profile_json.scoring.minimum_fit_threshold == 3.2


def test_update_candidate_profile_syncs_structured_schema_back_to_flat_fields() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    payload = CandidateProfilePayload(
        name="Structured Candidate",
        structured_profile_json=StructuredCandidateProfile(
            version="v1",
            targeting={
                "preferred_titles": ["founding operations lead"],
                "core_titles": ["founding operations lead"],
                "adjacent_titles": ["chief of staff"],
                "excluded_titles": ["intern"],
                "preferred_domains": ["developer tools"],
                "preferred_locations": ["san francisco"],
                "excluded_companies": ["BigCo"],
                "confirmed_skills": ["customer discovery", "sql"],
                "competencies": ["systems thinking"],
                "explicit_preferences": ["small teams", "remote-friendly"],
                "stage_preferences": ["seed"],
                "stretch_role_families": ["go_to_market"],
                "excluded_keywords": ["clearance required"],
                "seniority": {"guess": "senior", "minimum_band": "mid", "maximum_band": "staff"},
            },
            scoring={"minimum_fit_threshold": 3.4},
        ),
    )

    profile = update_candidate_profile(session, payload)
    refreshed = profile_to_payload(profile)

    assert profile.preferred_titles_json == ["founding operations lead"]
    assert profile.stage_preferences_json == ["seed"]
    assert profile.minimum_fit_threshold == 3.4
    assert profile.extracted_summary_json["structured_profile"]["scoring"]["minimum_fit_threshold"] == 3.4
    assert profile.extracted_summary_json["structured_profile"]["targeting"]["confirmed_skills"] == ["customer discovery", "sql"]
    assert profile.extracted_summary_json["structured_profile"]["targeting"]["competencies"] == ["systems thinking"]
    assert profile.extracted_summary_json["structured_profile"]["targeting"]["explicit_preferences"] == ["small teams", "remote-friendly"]
    assert refreshed.structured_profile_json is not None
    assert refreshed.structured_profile_json.targeting.preferred_domains == ["developer tools"]
    assert refreshed.confirmed_skills_json == ["customer discovery", "sql"]
    assert refreshed.competencies_json == ["systems thinking"]
    assert refreshed.explicit_preferences_json == ["small teams", "remote-friendly"]


def test_text_resume_upload_extraction() -> None:
    text, warnings = extract_text_from_resume_upload("resume.txt", b"chief of staff\noperations\n")
    assert "chief of staff" in text
    assert warnings == []


def test_pdf_resume_upload_extraction() -> None:
    objects = []

    def add_obj(content: bytes) -> None:
        objects.append(content)

    add_obj(b"<< /Type /Catalog /Pages 2 0 R >>")
    add_obj(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    add_obj(b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>")
    stream = b"BT /F1 24 Tf 72 72 Td (Hello PDF Resume) Tj ET"
    add_obj(b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream))
    add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf += f"{index} 0 obj\n".encode() + obj + b"\nendobj\n"
    xref_pos = len(pdf)
    pdf += f"xref\n0 {len(objects)+1}\n".encode()
    pdf += b"0000000000 65535 f \n"
    for offset in offsets[1:]:
        pdf += f"{offset:010d} 00000 n \n".encode()
    pdf += f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode()

    text, warnings = extract_text_from_resume_upload("resume.pdf", pdf)
    assert "Hello PDF Resume" in text
    assert warnings


def test_resume_preview_builds_structured_profile_from_upload() -> None:
    preview = preview_resume_upload(
        "resume.txt",
        b"Chief of Staff focused on AI and developer tools in San Francisco at Series A startups.",
    )

    assert preview["status"] == "complete"
    assert preview["missing_fields"] == []
    assert preview["candidate_profile"]["extracted_summary_json"]["resume_filename"] == "resume.txt"
    assert preview["candidate_profile"]["extracted_summary_json"]["extraction_status"] == "complete"
    assert preview["candidate_profile"]["structured_profile_json"]["targeting"]["preferred_titles"]
    assert "chief of staff" in preview["candidate_profile"]["preferred_titles_json"]


def test_resume_preview_marks_partial_extraction_without_failing() -> None:
    preview = preview_resume_text("resume.txt", "Experienced operator with strong execution.")

    assert preview["status"] == "partial"
    assert "preferred titles" in preview["missing_fields"]
    assert "preferred domains" in preview["missing_fields"]
    assert preview["candidate_profile"]["extracted_summary_json"]["extraction_status"] == "partial"
    assert preview["candidate_profile"]["extracted_summary_json"]["missing_fields"] == preview["missing_fields"]
    assert preview["candidate_profile"]["structured_profile_json"]["targeting"]["preferred_titles"]
    assert preview["warnings"]


def test_network_import_parses_csv_and_matches_company_referral_paths() -> None:
    network_payload = parse_network_csv(
        "linkedin.csv",
        (
            "full_name,current_company,title,relationship,linkedin_url,notes\n"
            "Alex Rivera,Linear,Product Operations,former teammate,https://linkedin.com/in/alex,Worked together on launch ops\n"
            "Casey Stone,Figma,Chief of Staff,warm intro,https://linkedin.com/in/casey,\n"
        ),
    )

    matches = match_referral_paths("Linear", network_payload)

    assert network_payload["import_summary"]["contact_count"] == 2
    assert matches == [
        {
            "contact_name": "Alex Rivera",
            "company": "Linear",
            "title": "Product Operations",
            "relationship": "former teammate",
            "profile_url": "https://linkedin.com/in/alex",
            "notes": "Worked together on launch ops",
            "location": "",
            "match_type": "direct_company",
            "adjacency_label": "Direct company contact",
            "path_summary": "Alex Rivera at Linear (former teammate)",
        }
    ]


def test_network_import_round_trips_through_profile_summary() -> None:
    network_payload = parse_network_csv(
        "network.csv",
        "name,company,title,relationship\nJamie Lee,Mercor,Ops Lead,former teammate\n",
    )

    merged = attach_network_import({"summary": "Existing profile"}, network_payload)

    assert extract_network_import(merged)["source_filename"] == "network.csv"
    assert extract_network_import(merged)["contacts"][0]["company"] == "Mercor"


def test_profile_data_inventory_surfaces_categories_provenance_and_processing_path() -> None:
    payload = CandidateProfilePayload(
        name="Privacy Test",
        raw_resume_text="Chief of staff with 8 years in AI.",
        preferred_titles_json=["chief of staff"],
        core_titles_json=["chief of staff"],
        preferred_domains_json=["ai"],
        preferred_locations_json=["remote"],
        confirmed_skills_json=["sql"],
        structured_profile_json=StructuredCandidateProfile(
            version="v1",
            targeting={"preferred_titles": ["chief of staff"], "core_titles": ["chief of staff"]},
            scoring={"minimum_fit_threshold": 2.8},
        ),
        extracted_summary_json=attach_network_import(
            {
                "summary": "Saved profile",
                "resume_filename": "resume.txt",
                "learning": {
                    "generated_queries": ["chief of staff ai"],
                    "title_weights": {"chief of staff": 1.2},
                },
            },
            parse_network_csv(
                "network.csv",
                "name,company,title,relationship\nJamie Lee,Mercor,Ops Lead,former teammate\n",
            ),
        ),
    )

    inventory = build_profile_data_inventory(payload.model_dump())
    inventory_by_key = {row["category_key"]: row for row in inventory}

    assert inventory_by_key["resume_text"]["stored"] is True
    assert inventory_by_key["resume_text"]["provenance"] == "Uploaded or pasted by the operator"
    assert inventory_by_key["network_contacts"]["item_count"] == 1
    assert inventory_by_key["network_contacts"]["processing_path"] == "local_only"
    assert inventory_by_key["profile_preferences"]["processing_path"] == "cloud_assisted"
    assert "chief of staff" in inventory_by_key["profile_preferences"]["example_values"]
    assert inventory_by_key["learning_state"]["stored"] is True
