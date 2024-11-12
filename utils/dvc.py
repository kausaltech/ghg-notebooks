from __future__ import annotations

import dvc_pandas

import settings


def get_repo(dvc_remote: str | None = None):
    if settings.GITHUB_USERNAME and settings.GITHUB_DATASET_TOKEN:
        creds = dvc_pandas.RepositoryCredentials(
            git_username=settings.GITHUB_USERNAME,
            git_token=settings.GITHUB_DATASET_TOKEN,
        )
    elif settings.GITHUB_SSH_PRIVATE_KEY and settings.GITHUB_SSH_PUBLIC_KEY:
        creds = dvc_pandas.RepositoryCredentials(
            git_ssh_private_key_file=str(settings.GITHUB_SSH_PRIVATE_KEY),
            git_ssh_public_key_file=str(settings.GITHUB_SSH_PUBLIC_KEY),
        )
    else:
        raise ValueError('No credentials for GitHub found')
    return dvc_pandas.Repository(
        repo_url=settings.DVC_PANDAS_REPOSITORY,
        dvc_remote=dvc_remote or settings.DVC_PANDAS_DVC_REMOTE,
        cache_prefix='ghg-notebooks',
        repo_credentials=creds,
    )


def update_dataset(path, df, dvc_remote: str | None = None):
    dataset = dvc_pandas.Dataset(
        df,
        identifier=path,
    )
    repo = get_repo(dvc_remote=dvc_remote)
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
