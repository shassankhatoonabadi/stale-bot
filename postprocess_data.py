import joblib
import numpy as np
import pandas as pd

from common import (
    cleanup_files,
    convert_dtypes,
    force_refresh,
    get_logger,
    get_path,
    import_dataframe,
    initialize,
    open_metadata,
    processed,
)

initialize()


@convert_dtypes
def add_core(dataframe):
    def find_core(events):
        events = events.assign(is_core=False)
        if (actor := events["actor"].iat[0]) != "ghost":  # noqa: F841
            events.loc[
                events["time"]
                >= min(
                    events.query("not is_contributor and closed_by == @actor")["closed_at"].min(),
                    events.query("merged_by == @actor")["merged_at"].min(),
                ),
                "is_core",
            ] = True
        return events

    return dataframe.groupby("actor", group_keys=False).apply(find_core)


def select_pulls(dataframe, query):
    return dataframe.query(query).index.unique("pull_number")


def export_dataset(project, dataset):
    dataset.to_csv(get_path("dataset", project))


def postprocess_data(project):
    logger = get_logger(__file__, modules={"sqlitedict": "WARNING"})
    logger.info(f"{project}: Postprocessing data")
    dataframe = import_dataframe(project)
    metadata = open_metadata(project)
    dataframe = add_core(dataframe)
    pulled = dataframe.query("event == 'pulled'")
    statistics = {
        "project": project,
        "language": metadata["language"],
        "stars": metadata["watchers"],
        "age": (pulled["time"].max() - pd.Timestamp(metadata["created_at"]).tz_localize(None)) / np.timedelta64(1, "M"),
        "contributors": pulled["actor"].nunique(),
        "maintainers": dataframe.query("is_core")["actor"].nunique(),
        "pulls": len(pulled),
        "open": len(select_pulls(pulled, "is_open")),
        "closed": len(select_pulls(pulled, "is_closed")),
        "merged": len(select_pulls(pulled, "is_merged")),
        "staled": len(select_pulls(pulled, "is_staled")),
        "staled+": len(select_pulls(pulled, "is_staled and is_merged")),
        "stale_closed": len(select_pulls(pulled, "is_stale_closed")),
        "stale_closed+": len(select_pulls(pulled, "is_stale_closed and is_merged")),
    }
    export_dataset(project, dataframe)
    return statistics


def export_statistics(statistics):
    file = get_path("statistics")
    pd.DataFrame(statistics).to_csv(file, header=not file.exists(), index=False, mode="a")


def main():
    fresh = force_refresh()
    if not cleanup_files("statistics", fresh):
        print("Skip refreshing statistics")
    projects = []
    for project in processed():
        if cleanup_files("dataset", fresh, project):
            projects.append(project)
        else:
            print(f"Skip postprocessing data for project {project}")
    with joblib.Parallel(n_jobs=-1, verbose=1) as parallel:
        export_statistics(parallel(joblib.delayed(postprocess_data)(project) for project in projects))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop postprocessing data")
        exit(1)
