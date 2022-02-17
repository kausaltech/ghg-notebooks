import dvc_pandas
import settings  # noqa


def get_repo():
    return dvc_pandas.Repository(
        settings.DVC_PANDAS_REPOSITORY,
        dvc_remote=settings.DVC_PANDAS_DVC_REMOTE,
    )


def update_dataset(path, df):
    dataset = dvc_pandas.Dataset(
        df,
        identifier=path,
    )
    repo = get_repo()
    repo.push_dataset(dataset)


def update_dataset_from_px(path, px_file):
    df = px_file.to_df(melt=True, dropna=True)
    # FIXME: Store metadata??
    update_dataset(path, df)


def load_datasets(identifiers):
    dfs = []
    repo = get_repo()
    if isinstance(identifiers, str):
        identifiers = [identifiers]
    for dsid in identifiers:
        df = repo.load_dataframe(dsid, skip_pull_if_exists=True)
        dfs.append(df)
    if len(dfs) == 1:
        return dfs[0]
    return dfs


def pull_datasets():
    repo = get_repo()
    repo.pull_datasets()
