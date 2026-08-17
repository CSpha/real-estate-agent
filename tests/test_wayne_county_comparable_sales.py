from datetime import datetime, timezone

from app.ingest.load_wayne_county_comparable_sales import (
    _exclude_bundle_duplicates,
    normalize_wayne_sale,
)


def epoch_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def auditor_sale(**overrides):
    record = {
        "OBJECTID": 1,
        "Parcel": "67-12345.000",
        "PPAddress": "123 N MAIN ST WOOSTER OH 44691",
        "PPAmount": 250000,
        "PPSaleDate": epoch_ms("2026-07-15"),
        "PPAcres": 0.25,
        "PPLivingArea": "1500",
        "PPYearBuilt": "1985",
        "PPDwellCount": 1,
        "PPTotalValue": 230000,
        "PPClassCode": 511,
        "PPClassNumber": "R",
        "PPListed": "CURRENT OWNER",
    }
    record.update(overrides)
    return record


def test_normalizes_single_family_auditor_sale():
    record = normalize_wayne_sale(auditor_sale())
    assert record["source_sale_id"] == "67-12345.000:2026-07-15"
    assert record["address"] == "123 N Main St"
    assert record["city"] == "Wooster"
    assert record["zip"] == "44691"
    assert record["property_type"] == "SingleFamilyResidence"
    assert record["arms_length"] is True


def test_proxy_rejects_mobile_home_and_implausible_price():
    assert normalize_wayne_sale(
        auditor_sale(PPClassCode=560)
    )["arms_length"] is False
    assert normalize_wayne_sale(
        auditor_sale(PPAmount=20000)
    )["arms_length"] is False
    assert normalize_wayne_sale(
        auditor_sale(PPAmount=2_000_000)
    )["arms_length"] is False


def test_bundle_keeps_only_largest_improved_parcel():
    first = normalize_wayne_sale(auditor_sale(PPLivingArea=1800))
    second = normalize_wayne_sale(
        auditor_sale(Parcel="67-12346.000", PPLivingArea=900)
    )
    assert _exclude_bundle_duplicates([first, second]) == 1
    assert first["arms_length"] is True
    assert second["arms_length"] is False


def test_condo_unit_address_is_parsed():
    record = normalize_wayne_sale(
        auditor_sale(
            PPAddress="1114 MINDY LN UNIT 1114 WOOSTER OH 44691",
            PPClassCode=550,
        )
    )
    assert record["address"] == "1114 Mindy Ln Unit 1114"
    assert record["property_type"] == "Condominium"


def test_neighboring_postal_city_and_duplicate_address_are_parsed():
    record = normalize_wayne_sale(
        auditor_sale(
            PPAddress=(
                "4791 DEERFIELD AVE NW UNIT 1/2 NORTH LAWRENCE OH 44666|"
                "4791 DEERFIELD AVE NW N LAWRENCE OH 44666"
            )
        )
    )
    assert record["city"] == "North Lawrence"
    assert record["zip"] == "44666"
