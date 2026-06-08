"""
jobs/combine_kpis.py
reads formatted parquet files from both sources and produces four kpi tables:

  1. salary_by_skill          — avg/median salary per skill (from so survey)
  2. skill_demand_ranking     — posting count per skill per day (from adzuna)
  3. remote_ratio_by_country  — work_mode distribution by country (from adzuna)
  4. experience_salary_matrix — salary bins crossed with years of experience (from so)

output: usage/kpis/{ds}/{kpi_name}.parquet
"""
from __future__ import annotations

import pandas as pd

from jobs.utils import s3 as s3_utils

SO_KEY = "formatted/survey/2024/so_survey.parquet"


def _load_all_adzuna(s3, ds: str) -> pd.DataFrame:
    """load the formatted adzuna parquet for the given date."""
    key = f"formatted/jobs/{ds}/adzuna.parquet"
    return s3_utils.get_parquet(s3, key)


def _kpi_salary_by_skill(survey_df: pd.DataFrame) -> pd.DataFrame:
    """
    from the so survey: explode the languagehaveworkedwith column
    and compute salary stats per skill.
    """
    df = survey_df[survey_df["salary_eur"].notna()].copy()
    df["skill"] = df["languages"].str.split(";")
    df = df.explode("skill")
    df["skill"] = df["skill"].str.strip()
    df = df[df["skill"].str.len() > 0]

    result = (
        df.groupby("skill")["salary_eur"]
        .agg(
            count="count",
            salary_avg="mean",
            salary_median="median",
            salary_p25=lambda x: x.quantile(0.25),
            salary_p75=lambda x: x.quantile(0.75),
        )
        .reset_index()
    )
    result["salary_avg"]    = result["salary_avg"].round(2)
    result["salary_median"] = result["salary_median"].round(2)
    result["salary_p25"]    = result["salary_p25"].round(2)
    result["salary_p75"]    = result["salary_p75"].round(2)
    return result[result["count"] >= 10].sort_values("salary_avg", ascending=False)


def _kpi_skill_demand(jobs_df: pd.DataFrame, ds: str) -> pd.DataFrame:
    """
    from adzuna: count how many postings mention each skill on this date.
    skills_str is a comma-separated string of matched skills per posting.
    """
    df = jobs_df[jobs_df["skills_str"].str.len() > 0].copy()
    df["skill"] = df["skills_str"].str.split(",")
    df = df.explode("skill")
    df["skill"] = df["skill"].str.strip()

    result = (
        df.groupby("skill")
        .size()
        .reset_index(name="posting_count")
        .sort_values("posting_count", ascending=False)
    )
    result["date"] = ds
    return result


def _kpi_remote_ratio(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    from adzuna: work_mode distribution (remote / hybrid / on-site) by country.
    """
    result = (
        jobs_df.groupby(["country", "work_mode"])
        .size()
        .reset_index(name="count")
    )
    totals = result.groupby("country")["count"].transform("sum")
    result["percentage"] = (result["count"] / totals * 100).round(1)
    return result.sort_values(["country", "count"], ascending=[True, False])


def _kpi_experience_salary(survey_df: pd.DataFrame) -> pd.DataFrame:
    """
    from so survey: average salary per experience bracket and dev type.
    useful for the 'salary vs seniority' kibana panel.
    """
    df = survey_df[
        survey_df["salary_eur"].notna() & survey_df["years_experience"].notna()
    ].copy()

    bins   = [0, 1, 3, 5, 10, 20, float("inf")]
    labels = ["<1yr", "1-3yrs", "3-5yrs", "5-10yrs", "10-20yrs", "20+yrs"]
    df["experience_bracket"] = pd.cut(df["years_experience"], bins=bins, labels=labels)

    result = (
        df.groupby(["experience_bracket", "primary_dev_type"])["salary_eur"]
        .agg(count="count", salary_avg="mean", salary_median="median")
        .reset_index()
    )
    result["salary_avg"]    = result["salary_avg"].round(2)
    result["salary_median"] = result["salary_median"].round(2)
    result["experience_bracket"] = result["experience_bracket"].astype(str)
    return result[result["count"] >= 5].sort_values(
        ["experience_bracket", "salary_avg"], ascending=[True, False]
    )


def combine_kpis(ds: str, **kwargs) -> None:
    """
    main callable for the airflow pythonoperator.
    loads both formatted sources and writes 4 kpi parquet files.
    """
    s3 = s3_utils.get_client()

    kpi_keys = {
        "salary_by_skill":         f"usage/kpis/{ds}/salary_by_skill.parquet",
        "skill_demand_ranking":    f"usage/kpis/{ds}/skill_demand_ranking.parquet",
        "remote_ratio_by_country": f"usage/kpis/{ds}/remote_ratio_by_country.parquet",
        "experience_salary_matrix":f"usage/kpis/{ds}/experience_salary_matrix.parquet",
    }

    # idempotency: if all kpis already exist, skip
    if all(s3_utils.key_exists(s3, k) for k in kpi_keys.values()):
        print(f"[SKIP] All KPIs already exist for {ds}")
        return

    print("Loading formatted sources ...")
    jobs_df   = _load_all_adzuna(s3, ds)
    survey_df = s3_utils.get_parquet(s3, SO_KEY)
    print(f"  Adzuna: {len(jobs_df)} postings | SO Survey: {len(survey_df)} respondents")

    kpi_fns = {
        "salary_by_skill":          lambda: _kpi_salary_by_skill(survey_df),
        "skill_demand_ranking":     lambda: _kpi_skill_demand(jobs_df, ds),
        "remote_ratio_by_country":  lambda: _kpi_remote_ratio(jobs_df),
        "experience_salary_matrix": lambda: _kpi_experience_salary(survey_df),
    }

    for name, fn in kpi_fns.items():
        out_key = kpi_keys[name]
        df = fn()
        s3_utils.put_parquet(s3, out_key, df)
        print(f"[OK] {name}: {len(df)} rows → {out_key}")
