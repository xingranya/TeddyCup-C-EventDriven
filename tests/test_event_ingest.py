from __future__ import annotations

import csv
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from pipeline.event_ingest import CollectedRecord, collect_events, normalize_events, publish_events


class EventIngestTestCase(unittest.TestCase):
    """事件采集链路测试。"""

    def test_collect_events_writes_raw_inbox_records(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            html_path = root / "detail.html"
            html_path.write_text(
                """
                <html>
                <head><title>政策测试标题</title></head>
                <body>
                <div>发布时间：2026-04-06 10:30:00</div>
                <p>这是用于测试的政策正文，内容长度足够，能够被采集脚本识别为有效事件。</p>
                </body>
                </html>
                """.strip(),
                encoding="utf-8",
            )
            input_path = root / "seed.csv"
            with input_path.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=["source_url"])
                writer.writeheader()
                writer.writerow({"source_url": html_path.as_uri()})

            output_path = collect_events(
                project_root=root,
                source="gov_cn",
                since_value="2026-04-01",
                until_value="2026-04-07",
                input_path=input_path,
            )

            rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["title"], "政策测试标题")
            self.assertIn("政策正文", rows[0]["content"])
            self.assertEqual(rows[0]["source_type"], "policy")

    def test_collect_events_uses_gov_feed_when_available(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch(
                "pipeline.event_ingest._fetch_json",
                return_value=[
                    {
                        "TITLE": "国务院关于产业链供应链安全的规定",
                        "URL": "https://www.gov.cn/zhengce/content/202604/content_7064837.htm",
                        "DOCRELPUBTIME": "2026-04-07",
                    }
                ],
            ), patch(
                "pipeline.event_ingest._build_record_from_url",
                return_value=CollectedRecord(
                    raw_id="gov-cn-1",
                    source="gov_cn",
                    source_type="policy",
                    source_name="中国政府网",
                    source_url="https://www.gov.cn/zhengce/content/202604/content_7064837.htm",
                    title="国务院关于产业链供应链安全的规定",
                    content="正文内容足够长，可用于测试政府网 JSON feed 的详情补抓逻辑。",
                    published_at="2026-04-07 00:00:00",
                    collected_at="2026-04-07 10:00:00",
                    collector_name="scripts/event_ingest.py",
                    collector_version="2026-04-v1",
                    batch="2026-04-07",
                ),
            ):
                output_path = collect_events(
                    project_root=root,
                    source="gov_cn",
                    since_value="2026-04-01",
                    until_value="2026-04-07",
                )

            rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["title"], "国务院关于产业链供应链安全的规定")
            self.assertEqual(rows[0]["published_at"], "2026-04-07 00:00:00")

    def test_normalize_events_generates_staging_and_review_queue(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stock_dir = root / "data" / "manual"
            stock_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame([{"stock_code": "600760", "stock_name": "中航沈飞"}]).to_csv(
                stock_dir / "stock_universe.csv",
                index=False,
            )
            raw_dir = root / "data" / "inbox" / "events_raw" / "macro_manual" / "2026-04-07"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_path = raw_dir / "records.jsonl"
            raw_path.write_text(
                json.dumps(
                    {
                        "raw_id": "macro-1",
                        "source": "macro_manual",
                        "source_type": "macro",
                        "source_name": "测试来源",
                        "source_url": "https://example.com/macro",
                        "title": "中航沈飞受地缘事件催化",
                        "content": "地缘事件发酵后，中航沈飞等军工产业链公司被频繁提及，具备明显市场影响，并且正文信息完整，足够进入人工审阅候选队列。",
                        "published_at": "2026-04-07 09:00:00",
                        "collected_at": "2026-04-07 09:30:00",
                        "collector_name": "scripts/event_ingest.py",
                        "collector_version": "2026-04-v1",
                        "batch": "2026-04-07",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            staging_path, queue_path = normalize_events(root, "macro_manual", "2026-04-07")

            staging_rows = [json.loads(line) for line in staging_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            queue_df = pd.read_csv(queue_path)
            self.assertEqual(len(staging_rows), 1)
            self.assertEqual(staging_rows[0]["review_status"], "pending")
            self.assertIn("中航沈飞", staging_rows[0]["entity_hits"])
            self.assertEqual(len(queue_df), 1)
            self.assertEqual(queue_df.iloc[0]["suggested_status"], "accepted")

    def test_publish_events_only_writes_accepted_records(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            staging_dir = root / "data" / "staging" / "events" / "policy"
            staging_dir.mkdir(parents=True, exist_ok=True)
            staging_path = staging_dir / "2026-04-07_gov_cn.jsonl"
            candidates = [
                {
                    "dedupe_key": "accepted-1",
                    "raw_id": "gov-1",
                    "source": "gov_cn",
                    "source_type": "policy",
                    "source_name": "中国政府网",
                    "source_url": "https://example.com/1",
                    "title": "政策事件一",
                    "content": "政策事件一正文，长度足够。",
                    "published_at": "2026-04-07 10:00:00",
                    "collected_at": "2026-04-07 10:10:00",
                    "collector_name": "scripts/event_ingest.py",
                    "review_status": "pending",
                    "review_note": "",
                    "suggested_status": "accepted",
                    "entity_hits": "",
                    "entity_hit_count": 0,
                    "duplicate_suspect": False,
                    "batch": "2026-04-07",
                },
                {
                    "dedupe_key": "pending-1",
                    "raw_id": "gov-2",
                    "source": "gov_cn",
                    "source_type": "policy",
                    "source_name": "中国政府网",
                    "source_url": "https://example.com/2",
                    "title": "政策事件二",
                    "content": "政策事件二正文，长度足够。",
                    "published_at": "2026-04-07 11:00:00",
                    "collected_at": "2026-04-07 11:10:00",
                    "collector_name": "scripts/event_ingest.py",
                    "review_status": "pending",
                    "review_note": "",
                    "suggested_status": "accepted",
                    "entity_hits": "",
                    "entity_hit_count": 0,
                    "duplicate_suspect": False,
                    "batch": "2026-04-07",
                },
            ]
            staging_path.write_text(
                "\n".join(json.dumps(item, ensure_ascii=False) for item in candidates) + "\n",
                encoding="utf-8",
            )
            review_queue = root / "data" / "staging" / "events" / "review_queue.csv"
            review_queue.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "dedupe_key": "accepted-1",
                        "batch": "2026-04-07",
                        "source_type": "policy",
                        "review_status": "accepted",
                        "review_note": "",
                    },
                    {
                        "dedupe_key": "pending-1",
                        "batch": "2026-04-07",
                        "source_type": "policy",
                        "review_status": "pending",
                        "review_note": "",
                    },
                ]
            ).to_csv(review_queue, index=False)

            output_paths = publish_events(root, "policy", "2026-04-07")

            self.assertEqual(len(output_paths), 1)
            payload = json.loads(output_paths[0].read_text(encoding="utf-8"))
            self.assertEqual(len(payload), 1)
            self.assertEqual(payload[0]["title"], "政策事件一")

    def test_normalize_events_keeps_review_queue_header_when_no_candidates(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_dir = root / "data" / "inbox" / "events_raw" / "gov_cn" / "2026-04-07"
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / "records.jsonl").write_text("", encoding="utf-8")

            _, queue_path = normalize_events(root, "gov_cn", "2026-04-07")

            content = queue_path.read_text(encoding="utf-8")
            self.assertIn("dedupe_key", content)
            self.assertIn("review_status", content)


if __name__ == "__main__":
    unittest.main()
