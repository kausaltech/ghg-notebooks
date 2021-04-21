import dvc_pandas
import settings  # noqa


def update_dataset_from_px(path, px_file):
    df = px_file.to_df(melt=True, dropna=True)
    # FIXME: Store metadata??
    dvc_pandas.push_dataset(df, path, dvc_remote='kausal-s3')


def update_dataset(path, df):
    dvc_pandas.push_dataset(df, path, dvc_remote='kausal-s3')


def load_datasets(identifiers):
    dfs = []
    if isinstance(identifiers, str):
        identifiers = [identifiers]
    for dsid in identifiers:
        df = dvc_pandas.load_dataset(dsid)
        dfs.append(df)
    if len(dfs) == 1:
        return dfs[0]
    return dfs
