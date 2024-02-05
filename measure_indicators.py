import joblib
import numpy as np
import pandas as pd

from common import (
    cleanup_files,
    convert_dtypes,
    force_refresh,
    get_logger,
    get_path,
    import_dataset,
    import_features,
    initialize,
    measured,
    open_metadata,
)

initialize()


@convert_dtypes
def add_stale_warning(stales):
    def find_stale_warning(events):
        events["is_warning"] = events["event"].isin(["commented", "labeled"]) & ~(
            events.shift(-1)["event"].eq("closed")
            & (events.shift(-1)["time"] - events["time"]).le(np.timedelta64(1, "m"))
        )
        return events

    return stales.groupby("pull_number", group_keys=False).apply(find_stale_warning)


@convert_dtypes
def measure_activity(stales):
    def measure_month(events):
        return pd.Series(
            {
                "events": len(events),
                "staled": events.index.get_level_values("pull_number").nunique(),
                "warned": events.query("is_warning").index.get_level_values("pull_number").nunique(),
                "closed": events.query("event == 'closed'").index.get_level_values("pull_number").nunique(),
            }
        )

    activity = stales.groupby("month", group_keys=False).apply(measure_month)
    return activity.reindex(range(activity.index.min(), activity.index.max() + 1), fill_value=0)


def export_features_fixed(project, features):
    features.reset_index().set_index("project").to_csv(get_path("features_fixed", project))


def export_activity(project, activity):
    activity.assign(project=project).reset_index().set_index("project").to_csv(get_path("activity", project))


def export_indicators(project, indicators):
    indicators.assign(project=project).set_index("project").to_csv(get_path("indicators", project))


def measure_indicators(project):
    logger = get_logger(__file__, modules={"sqlitedict": "WARNING"})
    logger.info(f"{project}: Measuring indicators")
    dataset = import_dataset(project)
    features = import_features(project)
    metadata = open_metadata(project)
    stales = dataset.query("is_stale").copy()
    if project == "automattic/wp-calypso":
        first_stale = pd.Timestamp("2019-04-20 05:46:48")
        stales = stales.query("time >= @first_stale").copy()
    else:
        first_stale = stales["time"].min()
    last_stale = stales["time"].max()
    stales["month"] = (stales["time"] - first_stale) // np.timedelta64(1, "M")
    stales = add_stale_warning(stales)
    activity = measure_activity(stales)
    characteristics = features.select_dtypes(include="number").columns
    features = features[features[characteristics].ge(0).all(axis="columns")].copy()
    features["opened_month"] = (features["opened_at"] - first_stale) // np.timedelta64(1, "M")
    features["resolved_month"] = (features["resolved_at"] - first_stale) // np.timedelta64(1, "M")
    monthly = pd.DataFrame()
    for month in range(
        features["opened_month"].min(), int(features[["opened_month", "resolved_month"]].max().max() + 1)
    ):
        opened = features.query("opened_month == @month")
        resolved = features.query("resolved_month == @month")
        monthly.loc[month, "opened_pulls"] = len(opened)
        monthly.loc[month, "merged_pulls"] = len(resolved.query("is_merged"))
        monthly.loc[month, "closed_pulls"] = len(resolved.query("is_closed"))
        monthly.loc[month, "active_contributors"] = opened["contributor"].nunique()
    monthly["open_pulls"] = (
        (monthly["opened_pulls"] - monthly["merged_pulls"] - monthly["closed_pulls"]).cumsum().shift(1, fill_value=0)
    )
    monthly["workload"] = monthly["opened_pulls"] + monthly["open_pulls"]
    monthly.index.name = "month"
    activity = monthly.join(activity).fillna(0)
    indicators = (
        features.groupby(["resolved_month", "is_merged"], as_index=False)
        .agg(dict.fromkeys(characteristics, "mean"))
        .merge(activity, left_on="resolved_month", right_index=True)
    )
    indicators["time"] = indicators["resolved_month"] + abs(indicators["resolved_month"].min()) + 1
    indicators["adoption"] = indicators["resolved_month"] >= 0
    indicators["time_since_adoption"] = indicators["resolved_month"].apply(lambda month: month + 1 if month >= 0 else 0)
    indicators["first_stale_time"] = first_stale
    indicators["last_stale_time"] = last_stale
    indicators["stale_activity_period"] = (last_stale - first_stale) / np.timedelta64(1, "M")
    indicators["age_at_adoption"] = (
        first_stale - pd.Timestamp(metadata["created_at"]).tz_localize(None)
    ) / np.timedelta64(1, "M")
    features_before = features.query("opened_month < 0")
    indicators["pulls_at_adoption"] = len(features_before)
    indicators["contributors_at_adoption"] = features_before["contributor"].nunique()
    indicators["maintainers_at_adoption"] = dataset.query("time < @first_stale and is_core")["actor"].nunique()
    export_features_fixed(project, features)
    export_activity(project, activity)
    export_indicators(project, indicators)


def main():
    projects = []
    for project in measured():
        if cleanup_files(["features_fixed", "activity", "indicators"], force_refresh(), project):
            projects.append(project)
        else:
            print(f"Skip measuring indicators for project {project}")
    with joblib.Parallel(n_jobs=-1, verbose=1) as parallel:
        parallel(joblib.delayed(measure_indicators)(project) for project in projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop measuring indicators")
        exit(1)
