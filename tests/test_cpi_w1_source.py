import json
import tempfile
import unittest
from pathlib import Path

import httpx

from src.cpi_w1_source import (
    BlsCpiSourceClient,
    BlsCpiSourceContract,
    SourceFailureKind,
    SourceFetchError,
    SourceLocator,
)


CONTRACT_PATH = Path("config/cpi_w1_source_contract.json")


class CpiW1SourceClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = BlsCpiSourceContract.from_json(CONTRACT_PATH)

    def locator(self, url: str, max_bytes: int = 1024) -> SourceLocator:
        return SourceLocator(
            key="TEST",
            url=url,
            artifact_contract_kind="CPI_RELEASE_HTML",
            surface_role="RELEASE_EVIDENCE",
            max_bytes=max_bytes,
        )

    def client_for(self, handler) -> BlsCpiSourceClient:
        transport = httpx.MockTransport(handler)
        client = httpx.Client(
            transport=transport,
            follow_redirects=False,
            headers={"Accept-Encoding": "identity"},
        )
        return BlsCpiSourceClient(self.contract, client=client)

    def test_contract_pins_cpi_schedule_as_authoritative(self) -> None:
        schedule = self.contract.locators["CPI_SCHEDULE_HTML"]
        ics = self.contract.locators["BLS_GLOBAL_ICS"]
        self.assertEqual(schedule.surface_role, "AUTHORITATIVE")
        self.assertEqual(ics.surface_role, "FALLBACK_CORROBORATION")

    def test_schedule_role_is_derived_from_versioned_artifact_contract(self) -> None:
        self.assertEqual(
            self.contract.surface_role_for_artifact_kind("CPI_SCHEDULE_HTML"),
            "AUTHORITATIVE",
        )
        self.assertEqual(
            self.contract.surface_role_for_artifact_kind("BLS_GLOBAL_ICS"),
            "FALLBACK_CORROBORATION",
        )
        self.assertFalse(
            self.contract.capture_chronology_allowed_for_artifact_kind(
                "CPI_SCHEDULE_HTML"
            )
        )
        self.assertFalse(
            self.contract.capture_chronology_allowed_for_artifact_kind(
                "BLS_GLOBAL_ICS"
            )
        )

    def test_revised_release_dates_are_explicit_authoritative_cancellation_evidence(self) -> None:
        locator = self.contract.locators["BLS_REVISED_RELEASE_DATES_HTML"]
        self.assertEqual(locator.surface_role, "AUTHORITATIVE")
        self.assertEqual(
            locator.artifact_contract_kind,
            "BLS_REVISED_RELEASE_DATES_HTML",
        )
        self.assertTrue(locator.requires_reference_month_validation)
        self.assertTrue(locator.requires_program_scope_validation)
        self.assertTrue(locator.explicit_cancellation_evidence)
        self.assertFalse(locator.capture_chronology_allowed)
        self.assertEqual(
            self.contract.surface_role_for_artifact_kind(
                "BLS_REVISED_RELEASE_DATES_HTML"
            ),
            "AUTHORITATIVE",
        )

    def test_contract_pins_current_table1_xlsx_as_corroboration_only(self) -> None:
        raw = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        locator = raw["locators"]["CURRENT_CPI_TABLE1_XLSX"]
        self.assertEqual(
            locator["url"],
            "https://www.bls.gov/web/cpi/cpipress1.xlsx",
        )
        self.assertEqual(
            locator["surface_role"],
            "CORROBORATING_REPRESENTATION",
        )
        self.assertTrue(locator["mutable_current_locator"])
        self.assertTrue(locator["requires_reference_month_validation"])
        self.assertFalse(locator["historical_as_released_authority"])

    def test_dynamic_correction_contract_has_no_invented_locator(self) -> None:
        raw = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        correction = raw["dynamic_artifact_contracts"]["CPI_CORRECTION_HTML"]
        self.assertEqual(correction["surface_role"], "CORRECTION_EVIDENCE")
        self.assertEqual(correction["locator_status"], "NOT_YET_FROZEN")
        self.assertTrue(correction["requires_reference_month_validation"])
        self.assertTrue(correction["requires_explicit_disclosure_scope"])
        self.assertNotIn("url", correction)
        self.assertEqual(
            self.contract.surface_role_for_artifact_kind("CPI_CORRECTION_HTML"),
            "CORRECTION_EVIDENCE",
        )

    def test_non_https_is_rejected_before_network(self) -> None:
        with self.assertRaises(SourceFetchError) as caught:
            self.contract.validate_url("http://www.bls.gov/news.release/cpi.nr0.htm")
        self.assertEqual(caught.exception.kind, SourceFailureKind.POLICY)

    def test_injected_client_cannot_enable_automatic_redirects(self) -> None:
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            if len(seen) == 1:
                return httpx.Response(
                    302,
                    headers={"Location": "https://example.com/cpi"},
                    request=request,
                )
            return httpx.Response(200, content=b"should-not-be-fetched", request=request)

        injected = httpx.Client(
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
        )
        client = BlsCpiSourceClient(self.contract, client=injected)
        with self.assertRaises(SourceFetchError) as caught:
            client.fetch(
                self.locator("https://www.bls.gov/news.release/cpi.nr0.htm")
            )
        self.assertEqual(caught.exception.kind, SourceFailureKind.POLICY)
        self.assertEqual(len(seen), 1)

    def test_redirect_to_non_bls_host_is_rejected(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                302,
                headers={"Location": "https://example.com/cpi"},
                request=request,
            )

        client = self.client_for(handler)
        with self.assertRaises(SourceFetchError) as caught:
            client.fetch(
                self.locator("https://www.bls.gov/news.release/cpi.nr0.htm")
            )
        self.assertEqual(caught.exception.kind, SourceFailureKind.POLICY)

    def test_oversized_content_length_is_rejected(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"Content-Length": "4096"},
                content=b"x",
                request=request,
            )

        client = self.client_for(handler)
        with self.assertRaises(SourceFetchError) as caught:
            client.fetch(
                self.locator(
                    "https://www.bls.gov/news.release/cpi.nr0.htm",
                    max_bytes=100,
                )
            )
        self.assertEqual(caught.exception.kind, SourceFailureKind.OVERSIZED)

    def test_streamed_oversize_is_rejected(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"x" * 101, request=request)

        client = self.client_for(handler)
        with self.assertRaises(SourceFetchError) as caught:
            client.fetch(
                self.locator(
                    "https://www.bls.gov/news.release/cpi.nr0.htm",
                    max_bytes=100,
                )
            )
        self.assertEqual(caught.exception.kind, SourceFailureKind.OVERSIZED)

    def test_unexpected_compression_is_rejected(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            import gzip

            return httpx.Response(
                200,
                headers={"Content-Encoding": "gzip"},
                content=gzip.compress(b"not-relevant"),
                request=request,
            )

        client = self.client_for(handler)
        with self.assertRaises(SourceFetchError) as caught:
            client.fetch(
                self.locator("https://www.bls.gov/news.release/cpi.nr0.htm")
            )
        self.assertEqual(caught.exception.kind, SourceFailureKind.PROTOCOL)

    def test_429_is_retryable_operational_failure(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429, request=request)

        client = self.client_for(handler)
        with self.assertRaises(SourceFetchError) as caught:
            client.fetch(
                self.locator("https://www.bls.gov/news.release/cpi.nr0.htm")
            )
        self.assertEqual(caught.exception.kind, SourceFailureKind.RETRYABLE)
        self.assertEqual(caught.exception.status_code, 429)

    def test_404_is_not_economic_cancellation(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, request=request)

        client = self.client_for(handler)
        with self.assertRaises(SourceFetchError) as caught:
            client.fetch(
                self.locator("https://www.bls.gov/news.release/cpi.nr0.htm")
            )
        self.assertEqual(caught.exception.kind, SourceFailureKind.NOT_FOUND)
        self.assertNotIn("CANCELED", str(caught.exception))
        self.assertNotIn("NO_RELEASE_EXPECTED", str(caught.exception))

    def test_fetch_does_not_follow_html_subresources(self) -> None:
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(
                200,
                content=b'<html><img src="https://evil.example/a.png"></html>',
                request=request,
            )

        client = self.client_for(handler)
        response = client.fetch(
            self.locator("https://www.bls.gov/news.release/cpi.nr0.htm")
        )
        self.assertEqual(len(seen), 1)
        self.assertIn(b"evil.example", response.body)

    def test_contract_file_contains_no_credentials(self) -> None:
        import json

        payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        forbidden_keys = {
            "password",
            "secret",
            "api_key",
            "apikey",
            "authorization",
            "access_token",
            "refresh_token",
            "client_secret",
        }

        def walk(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    self.assertNotIn(key.lower(), forbidden_keys)
                    walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(payload)


if __name__ == "__main__":
    unittest.main()
