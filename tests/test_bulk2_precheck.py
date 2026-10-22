"""Bulk2 starts only when an incremental query matches more than one record."""

import asyncio
import contextlib
import io
import json
import unittest
from copy import deepcopy
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from singer import metadata

from tap_salesforce import do_sync
from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.credentials import PasswordCredentials
from tap_salesforce.salesforce.exceptions import TapSalesforceExceptionError


def catalog_entry(name="Account", replication_key="SystemModstamp"):
    stream_metadata = {
        "selected": True,
        "replication-method": "INCREMENTAL" if replication_key else "FULL_TABLE",
        "table-key-properties": ["Id"],
    }
    if replication_key:
        stream_metadata["replication-key"] = replication_key
    return {
        "stream": name,
        "tap_stream_id": name,
        "metadata": metadata.to_list({(): stream_metadata}),
        "schema": {
            "type": "object",
            "properties": {
                "Id": {"type": "string"},
                "SystemModstamp": {"type": "string", "format": "date-time"},
            },
        },
    }


class Bulk2PrecheckTests(unittest.TestCase):
    def setUp(self):
        self.sf = Salesforce(
            credentials=PasswordCredentials("user", "password", "token"),
            api_type="BULK2",
            select_fields_by_default=True,
            default_start_date="2020-01-01T00:00:00Z",
        )
        self.sf.auth._instance_url = "https://example.salesforce.com"
        self.sf._make_request = self.request
        self.count = 0
        self.failed_check = False
        self.invalid_result = False
        self.probes = []
        self.jobs = []

    def request(self, method, url, **kwargs):
        if method == "POST" and url.endswith("/composite/batch"):
            queries = [
                parse_qs(urlparse(item["url"]).query)["q"][0]
                for item in json.loads(kwargs["body"])["batchRequests"]
            ]
            self.probes.append(queries)
            results = []
            for query in queries:
                limit = int(query.rsplit("LIMIT ", 1)[1])
                records = [{"Id": str(index)} for index in range(min(self.count, limit))]
                result = {"done": True, "totalSize": len(records), "records": records}
                if self.invalid_result:
                    result["totalSize"] += 1
                results.append({"statusCode": 400 if self.failed_check else 200, "result": result})
            return SimpleNamespace(json=lambda: {"results": results})
        if method == "POST" and url.endswith("/jobs/query"):
            self.jobs.append(json.loads(kwargs["body"]))
            return SimpleNamespace(json=lambda: {"id": "job"})
        if method == "GET" and url.endswith("/jobs/query/job"):
            return SimpleNamespace(json=lambda: {"state": "JobComplete"})
        if method == "GET" and url.endswith("/jobs/query/job/results"):
            content = "Id,SystemModstamp\n" + "".join(
                f"{index},2026-10-21T12:00:00Z\n" for index in range(self.count)
            )
            return SimpleNamespace(headers={"Sforce-Locator": "null"}, content=content.encode())
        self.fail(f"Unexpected API request: {method} {url}")

    def run_sync(self, state):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        output = io.StringIO()
        try:
            with contextlib.redirect_stdout(output):
                do_sync(self.sf, {"streams": [catalog_entry()]}, state)
        finally:
            if not loop.is_closed():
                loop.close()
            asyncio.set_event_loop(None)
        return [json.loads(line) for line in output.getvalue().splitlines()]

    def test_sync_runs_only_above_one_match_and_bulk_download_is_unlimited(self):
        for count in (0, 1, 2, 3):
            with self.subTest(count=count):
                self.count = count
                self.jobs.clear()
                self.probes.clear()
                state = {"bookmarks": {"Account": {"SystemModstamp": "2026-10-21T12:00:00Z"}}}
                original_state = deepcopy(state)

                messages = self.run_sync(state)

                self.assertEqual(int(count > 1), len(self.jobs))
                self.assertEqual(
                    [["SELECT Id FROM Account WHERE SystemModstamp >= 2026-10-21T12:00:00Z LIMIT 2"]], self.probes
                )
                records = [message for message in messages if message["type"] == "RECORD"]
                self.assertEqual(count if count > 1 else 0, len(records))
                if count > 1:
                    self.assertEqual("queryAll", self.jobs[0]["operation"])
                    self.assertNotIn("LIMIT", self.jobs[0]["query"])
                else:
                    self.assertEqual(original_state, state)

    def test_discovery_keeps_one_record_threshold_without_dates(self):
        for count, expected in ((0, []), (1, ["Account"]), (3, ["Account"])):
            self.count = count
            self.assertEqual(expected, self.sf.get_nonempty_objects(["Account"]))
        self.assertEqual([["SELECT Id FROM Account LIMIT 1"]] * 3, self.probes)

    def test_initial_sync_also_skips_one_record(self):
        self.count = 1
        state = {}

        self.run_sync(state)

        self.assertEqual([], self.jobs)
        self.assertEqual({}, state)
        self.assertEqual(
            [["SELECT Id FROM Account WHERE SystemModstamp >= 2020-01-01T00:00:00+00:00 LIMIT 2"]], self.probes
        )

    def test_checks_streams_in_batches_of_25(self):
        self.count = 2
        entries = [catalog_entry(f"Object{index}__c") for index in range(26)]

        self.assertEqual(entries, self.sf.get_nonempty_objects(entries, state={}))

        self.assertEqual([25, 1], [len(batch) for batch in self.probes])

    def test_full_table_and_resumed_streams_bypass_checks(self):
        entries = [catalog_entry("Full", replication_key=None), catalog_entry("Resumed")]
        state = {"bookmarks": {"Resumed": {"JobID": "existing"}}}

        self.assertEqual(entries, self.sf.get_nonempty_objects(entries, state=state))
        self.assertEqual([], self.probes)

    def test_other_api_types_bypass_checks(self):
        for api_type in ("REST", "BULK"):
            with self.subTest(api_type=api_type):
                self.sf.api_type = api_type
                self.sf.query = lambda *_args: iter(())
                self.run_sync({})
                self.assertEqual([], self.probes)

    def test_failed_sync_check_raises_while_discovery_skips(self):
        self.failed_check = True

        with self.assertRaises(TapSalesforceExceptionError):
            self.sf.get_nonempty_objects([catalog_entry()], state={})
        self.assertEqual([], self.sf.get_nonempty_objects(["Account"]))

    def test_invalid_check_result_never_launches_bulk_job(self):
        self.invalid_result = True
        state = {"bookmarks": {"Account": {"SystemModstamp": "2026-10-21T12:00:00Z"}}}
        original_state = deepcopy(state)

        with self.assertRaises(TapSalesforceExceptionError):
            self.run_sync(state)

        self.assertEqual([], self.jobs)
        self.assertEqual(original_state, state)


if __name__ == "__main__":
    unittest.main()
