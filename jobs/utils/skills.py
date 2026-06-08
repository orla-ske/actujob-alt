"""
jobs/utils/skills.py
Extracts a list of known tech skills from a free-text job description.
Uses simple keyword matching — can be upgraded to NLP later.
"""
from __future__ import annotations

import re

# ── Skill taxonomy ─────────────────────────────────────────────────────────────
# Keys are canonical skill names stored in the output.
# Values are regex patterns matched case-insensitively against job descriptions.
SKILLS: dict[str, str] = {
    # Languages
    "Python":       r"\bpython\b",
    "JavaScript":   r"\bjavascript\b|\bjs\b",
    "TypeScript":   r"\btypescript\b|\bts\b",
    "Java":         r"\bjava\b(?!script)",
    "Kotlin":       r"\bkotlin\b",
    "Go":           r"\bgolang\b|\bgo\b",
    "Rust":         r"\brust\b",
    "C++":          r"\bc\+\+\b|\bcpp\b",
    "C#":           r"\bc#\b|\bdotnet\b|\.net\b",
    "SQL":          r"\bsql\b",
    "Scala":        r"\bscala\b",
    "R":            r"\br programming\b|\br language\b",
    "PHP":          r"\bphp\b",
    "Ruby":         r"\bruby\b",
    "Swift":        r"\bswift\b",

    # Frontend
    "React":        r"\breact(?:\.js|js)?\b",
    "Vue":          r"\bvue(?:\.js|js)?\b",
    "Angular":      r"\bangular\b",
    "Next.js":      r"\bnext(?:\.js|js)\b",
    "HTML/CSS":     r"\bhtml\b|\bcss\b|\bsass\b",

    # Backend / frameworks
    "Node.js":      r"\bnode(?:\.js|js)?\b",
    "FastAPI":      r"\bfastapi\b",
    "Django":       r"\bdjango\b",
    "Spring Boot":  r"\bspring\s*boot\b|\bspring\b",
    "Flask":        r"\bflask\b",

    # Data & ML
    "Spark":        r"\bapache\s*spark\b|\bpyspark\b",
    "Kafka":        r"\bkafka\b",
    "Airflow":      r"\bairflow\b",
    "dbt":          r"\bdbt\b",
    "Pandas":       r"\bpandas\b",
    "TensorFlow":   r"\btensorflow\b",
    "PyTorch":      r"\bpytorch\b",
    "Scikit-learn": r"\bscikit[\s-]?learn\b|\bsklearn\b",
    "LLM":          r"\bllm\b|\blarge\s*language\s*model\b",

    # Databases
    "PostgreSQL":   r"\bpostgres(?:ql)?\b",
    "MySQL":        r"\bmysql\b",
    "MongoDB":      r"\bmongodb\b",
    "Redis":        r"\bredis\b",
    "Elasticsearch":r"\belasticsearch\b",
    "Cassandra":    r"\bcassandra\b",
    "Snowflake":    r"\bsnowflake\b",
    "BigQuery":     r"\bbigquery\b",

    # Cloud & DevOps
    "AWS":          r"\baws\b|\bamazon\s*web\s*services\b",
    "GCP":          r"\bgcp\b|\bgoogle\s*cloud\b",
    "Azure":        r"\bazure\b",
    "Docker":       r"\bdocker\b",
    "Kubernetes":   r"\bkubernetes\b|\bk8s\b",
    "Terraform":    r"\bterraform\b",
    "CI/CD":        r"\bci/cd\b|\bgithub\s*actions\b|\bjenkins\b|\bgitlab\s*ci\b",
}

# Compiled patterns for performance
_COMPILED = {skill: re.compile(pattern, re.IGNORECASE) for skill, pattern in SKILLS.items()}


def extract_skills(text: str) -> list[str]:
    """Return a sorted list of skills found in the given text."""
    if not text:
        return []
    return sorted(skill for skill, pattern in _COMPILED.items() if pattern.search(text))


def classify_work_mode(text: str, contract_type: str = "") -> str:
    """
    Classify a job posting as 'remote', 'hybrid', or 'on-site'
    based on description and contract_type fields.
    """
    combined = f"{text} {contract_type}".lower()
    if re.search(r"\bfull[- ]?remote\b|\b100\s*%\s*remote\b|\bfully\s*remote\b", combined):
        return "remote"
    if re.search(r"\bhybrid\b|\btélétravail partiel\b|\bpartial remote\b", combined):
        return "hybrid"
    if re.search(r"\bremote\b|\btélétravail\b|\bwork from home\b|\bwfh\b", combined):
        return "remote"
    return "on-site"
