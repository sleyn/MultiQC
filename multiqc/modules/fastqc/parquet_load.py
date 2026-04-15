"""
Custom parquet section handling for the FastQC module.

This file is automatically discovered and used by the parquet loading system
(load_multiqc_data.py) when restoring MultiQC reports from parquet files.

Convention
----------
* ``CUSTOM_MERGE_SECTION_ANCHORS`` – list of anchor **substrings**.
  A section whose anchor contains any of these strings will be handed to
  ``merge_section_content`` instead of the default concatenation logic.
* ``merge_section_content(existing_content, new_content) -> Optional[str]``
  Takes the HTML ``content`` strings of two sections and returns a single
  merged HTML string, or ``None`` to fall back to the default merge.
"""

import json
import logging
import re
from typing import Optional

log = logging.getLogger(__name__)

# ── anchor substrings that need custom HTML-content merging ──────────────────
CUSTOM_MERGE_SECTION_ANCHORS = ["per_base_sequence_content"]

# ── regex for the embedded JSON data block ───────────────────────────────────
_SCRIPT_RE = re.compile(
    r'<script[^>]+class="fastqc_seq_content"[^>]*>(.*?)</script>',
    re.DOTALL,
)


def merge_section_content(existing_content: str, new_content: str) -> Optional[str]:
    """Merge two Per Base Sequence Content HTML sections.

    Each section embeds a ``<script type="application/json"
    class="fastqc_seq_content">`` block whose content is a JSON array:

    .. code-block:: json

        ["<module_anchor>", {"<sample_name>": {"<pos>": {...}}, ...}]

    The function extracts the sample dicts from both sections, merges them
    (samples from *new_content* win on collision), and returns a copy of
    *existing_content* with the script block replaced by the merged data.

    Returns ``None`` when merging cannot be performed (missing/malformed data),
    which causes the caller to fall back to the default merge strategy.
    """
    existing_match = _SCRIPT_RE.search(existing_content)
    new_match = _SCRIPT_RE.search(new_content)

    if not existing_match or not new_match:
        log.warning(
            "FastQC parquet_load: could not find fastqc_seq_content script "
            "tags in sections being merged – falling back to default merge"
        )
        return None

    try:
        existing_data = json.loads(existing_match.group(1))
        new_data = json.loads(new_match.group(1))
    except json.JSONDecodeError as exc:
        log.warning(
            "FastQC parquet_load: JSON decode error in fastqc_seq_content "
            "script tags: %s – falling back to default merge",
            exc,
        )
        return None

    # Expected format: [module_anchor: str, sample_data: dict]
    if not (
        isinstance(existing_data, list)
        and len(existing_data) == 2
        and isinstance(existing_data[1], dict)
        and isinstance(new_data, list)
        and len(new_data) == 2
        and isinstance(new_data[1], dict)
    ):
        log.warning(
            "FastQC parquet_load: unexpected JSON structure in fastqc_seq_content – falling back to default merge"
        )
        return None

    # Merge sample dicts; samples from the *new* section win on key collision
    merged_samples: dict = {**existing_data[1], **new_data[1]}
    module_anchor: str = existing_data[0]

    merged_dump = json.dumps([module_anchor, merged_samples])

    # Replace the script block inside existing_content with merged data
    merged_content = _SCRIPT_RE.sub(
        f'<script type="application/json" class="fastqc_seq_content">{merged_dump}</script>',
        existing_content,
    )

    log.debug(
        "FastQC parquet_load: merged %d samples from two parquet sections (module anchor: %s)",
        len(merged_samples),
        module_anchor,
    )
    return merged_content
