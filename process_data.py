import joblib
import numpy as np
import pandas as pd

from common import (
    cleanup_files,
    convert_dtypes,
    force_refresh,
    get_logger,
    get_path,
    import_timelines,
    initialize,
    preprocessed,
)

initialize()


def chunk_timelines(project):
    groups = [group[1] for group in import_timelines(project).groupby("pull_number")]
    for indices in np.array_split(np.arange(len(groups)), joblib.cpu_count()):
        yield pd.concat([groups[index] for index in indices])


@convert_dtypes
def add_status(timelines):
    def find_status(timeline):
        pulled = timeline.query("event == 'pulled'")
        closed = timeline.query("event == 'closed'")
        timeline = timeline.assign(
            is_open=False,
            is_closed=False,
            is_merged=False,
            opened_at=pulled["time"].iat[0],
            closed_at=pd.NaT,
            merged_at=pd.NaT,
            closed_by=np.nan,
            merged_by=np.nan,
        )
        if pulled["state"].iat[0] == "closed":
            if not (merged := timeline.query("event == 'merged'")).empty:
                timeline["is_merged"] = True
                timeline["merged_at"] = merged["time"].iat[0]
                timeline["merged_by"] = merged["actor"].iat[0]
            elif not (commit_id := closed.query("commit_id.notna()")).empty:
                timeline["is_merged"] = True
                timeline["merged_at"] = commit_id["time"].iat[0]
                timeline["merged_by"] = commit_id["actor"].iat[0]
            elif not (referenced := timeline.query("referenced")).empty:
                timeline["is_merged"] = True
                timeline["merged_at"] = referenced["time"].iat[0]
                timeline["merged_by"] = referenced["actor"].iat[0]
            else:
                timeline["is_closed"] = True
                if not closed.empty:
                    timeline["closed_at"] = closed["time"].iat[-1]
                    timeline["closed_by"] = closed["actor"].iat[-1]
        else:
            timeline["is_open"] = True
        return timeline

    timelines = timelines.groupby("pull_number", group_keys=False).apply(find_status)
    timelines["resolved_at"] = timelines["merged_at"].fillna(timelines["closed_at"])
    timelines["resolved_by"] = timelines["merged_by"].fillna(timelines["closed_by"])
    return timelines.drop(columns=["state", "commit_id", "referenced"])


@convert_dtypes
def add_contributor(timelines):
    def find_contributor(timeline):
        timeline["is_contributor"] = timeline["actor"] == timeline.query("event == 'pulled'")["actor"].iat[0]
        return timeline

    return timelines.groupby("pull_number", group_keys=False).apply(find_contributor)


@convert_dtypes
def add_stale(timelines):
    def find_stale_action_closed(timeline):
        if not (action_closed := timeline.query("event == 'closed' and actor == 'github-actions[bot]'")).empty:
            if not timeline.query("is_stale_action and time <= @action_closed['time'].iat[0]").empty:
                timeline.loc[action_closed.index, "is_stale_action"] = True
        return timeline

    timelines["is_stale_bot"] = timelines["actor"] == "stale[bot]"
    timelines["is_stale_action"] = timelines["actor"].eq("github-actions[bot]") & (
        (timelines["event"].eq("commented") & timelines["body"].str.contains("stale", case=False))
        | (timelines["event"].isin(["labeled", "unlabeled"]) & timelines["label"].str.contains("stale", case=False))
    )
    timelines = timelines.groupby("pull_number", group_keys=False).apply(find_stale_action_closed)
    timelines["is_stale"] = timelines["is_stale_bot"] | timelines["is_stale_action"]
    return timelines.drop(columns=["label", "body"])


@convert_dtypes
def add_staled(timelines):
    def find_staled(timeline):
        timeline = timeline.assign(is_staled=False, first_staled_at=pd.NaT, last_staled_at=pd.NaT)
        if not (stale := timeline.query("is_stale")).empty:
            timeline["is_staled"] = True
            timeline["first_staled_at"] = stale["time"].iat[0]
            timeline["last_staled_at"] = stale["time"].iat[-1]
        return timeline

    return timelines.groupby("pull_number", group_keys=False).apply(find_staled)


@convert_dtypes
def add_stale_closed(timelines):
    def find_stale_closed(timeline):
        timeline = timeline.assign(is_stale_closed=False, first_stale_closed_at=pd.NaT, last_stale_closed_at=pd.NaT)
        if not (stale_closed := timeline.query("event == 'closed' and is_stale")).empty:
            timeline["is_stale_closed"] = True
            timeline["first_stale_closed_at"] = stale_closed["time"].iat[0]
            timeline["last_stale_closed_at"] = stale_closed["time"].iat[-1]
        return timeline

    return timelines.groupby("pull_number", group_keys=False).apply(find_stale_closed)


def process_chunk(chunk):
    chunk = add_status(chunk)
    chunk = add_contributor(chunk)
    chunk = add_stale(chunk)
    chunk = add_staled(chunk)
    chunk = add_stale_closed(chunk)
    return chunk


def export_dataframe(project, chunks):
    pd.concat(chunks).to_csv(get_path("dataframe", project))


def process_data(project):
    logger = get_logger(__file__)
    logger.info(f"{project}: Processing data")
    with joblib.Parallel(n_jobs=-1, verbose=1) as parallel:
        export_dataframe(project, parallel(joblib.delayed(process_chunk)(chunk) for chunk in chunk_timelines(project)))


def main():
    projects = []
    for project in preprocessed():
        if cleanup_files("dataframe", force_refresh(), project):
            projects.append(project)
        else:
            print(f"Skip processing data for project {project}")
    for project in projects:
        process_data(project)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop processing data")
        exit(1)
