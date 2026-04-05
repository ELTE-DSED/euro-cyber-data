"""Generate word clouds from raw JSON data."""

import argparse
import json
import re
from pathlib import Path
from typing import Iterable, List, Dict, Optional

from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

from src.utils.config import RAW_DATA_DIR, JOB_POSTINGS_FILE

RAW_FIELDS = {
    "title": ["Title", "Job Title", "title"],
    "description": ["Description", "description"],
    "skill": ["Skill", "skill"],
}


def pick_field(record: Dict, candidates: List[str]) -> Optional[str]:
    for key in candidates:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def normalize_text(text: str) -> str:
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_skill_field(value: str) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = value
    else:
        parts = [p.strip() for p in str(value).split(",")]
    parts = [normalize_text(p) for p in parts if p]
    return " ".join(parts)


def collect_raw_texts(raw_path: Path) -> Dict[str, List[str]]:
    with raw_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    collected = {"title": [], "description": [], "skill": []}
    for record in data:
        title = pick_field(record, RAW_FIELDS["title"])
        description = pick_field(record, RAW_FIELDS["description"])
        skill = pick_field(record, RAW_FIELDS["skill"])

        if title:
            collected["title"].append(normalize_text(title))
        if description:
            collected["description"].append(normalize_text(description))
        if skill:
            collected["skill"].append(normalize_skill_field(skill))

    return collected


def build_wordcloud(texts: Iterable[str], output_path: Path, max_words: int):
    text_blob = " ".join([t for t in texts if t])
    if not text_blob.strip():
        print(f"No text for {output_path.name}")
        return

    wc = WordCloud(
        width=1600,
        height=900,
        background_color="white",
        max_words=max_words,
        stopwords=STOPWORDS,
        collocations=False,
    )
    image = wc.generate(text_blob)

    plt.figure(figsize=(12, 7))
    plt.imshow(image, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Generate word clouds")
    parser.add_argument(
        "--output-dir",
        default=str(Path("data") / "wordclouds"),
        help="Output directory for images",
    )
    parser.add_argument("--max-words", type=int, default=150)
    parser.add_argument(
        "--raw-file",
        default=str(RAW_DATA_DIR / JOB_POSTINGS_FILE),
        help="Raw job postings JSON file",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)

    raw_texts = collect_raw_texts(Path(args.raw_file))
    for key, texts in raw_texts.items():
        build_wordcloud(texts, output_dir / f"raw_{key}.png", args.max_words)
    print("Raw word clouds created")


if __name__ == "__main__":
    main()
