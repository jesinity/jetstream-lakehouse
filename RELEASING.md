# Publishing a release

The `Release` workflow builds and checks distributions after offline CI passes
on Python 3.10–3.14. Both manual runs and GitHub releases publish to TestPyPI
and verify installation. A manual run ends there. A GitHub release continues
to the protected PyPI environment for approval, then publishes the same build
artifacts. Ordinary pushes and pull requests run CI only.
The workflow uses Trusted Publishing; no PyPI API token is stored in GitHub.

## One-time account setup

Register and verify accounts on PyPI and TestPyPI, then enable two-factor
authentication. In each account's Publishing page, add a pending GitHub publisher:

| Setting | TestPyPI | PyPI |
| --- | --- | --- |
| Publishing page | https://test.pypi.org/manage/account/publishing/ | https://pypi.org/manage/account/publishing/ |
| PyPI project name | `jetstream-lakehouse` | `jetstream-lakehouse` |
| Owner | `jesinity` | `jesinity` |
| Repository | `jetstream-lakehouse` | `jetstream-lakehouse` |
| Workflow filename | `release.yml` | `release.yml` |
| Environment | `testpypi` | `pypi` |

For an existing package that you own, add the publisher under that project's
Publishing settings instead. A pending publisher does not reserve the name;
the first successful upload creates the project.

In the GitHub repository's Settings → Environments, create `testpypi` and `pypi`.
Configure the `pypi` environment to allow release tags (`v*`). Where supported
by the repository plan, require a reviewer before production publication.

See the official [pending publisher guide](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
and [publishing guide](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

## First release and later updates

1. Set the same version in `pyproject.toml` and
   `src/jetstream_lakehouse/__init__.py`, then run `uv lock`.
2. Commit the source, packaging files, and workflows. Merge them into the
   repository's default branch so GitHub exposes the manual workflow.
3. In Actions → Release → Run workflow, select the intended release branch
   and click Run workflow. This publishes to **TestPyPI** after the checks
   succeed. The `testpypi` environment and its pending Trusted Publisher must
   already be configured with the values above. No API token is needed.
4. Wait for `verify-testpypi` to pass. It checks the wheel and source archive's
   SHA-256 hashes against this build, downloads the wheel from TestPyPI, and
   installs it in a fresh environment with dependencies from normal PyPI.
   It checks public imports, version consistency, and basic API construction.
   The manual run finishes without requesting production approval. You can
   also install the TestPyPI version to exercise your intended consumer.
5. Create a `v0.1.0` tag (or the matching new version) at that verified commit,
   and publish a GitHub release for the tag. The workflow checks that the tag
   and both version declarations agree, runs CI, and repeats the TestPyPI
   publication and verification. Existing TestPyPI files are accepted only
   when their hashes match this build exactly.
6. After verification passes, the **PyPI** job waits for your configured
   environment approval. In the workflow run, choose Review deployments →
   `pypi` → Approve and deploy. It uploads the same wheel and source archive
   checked against TestPyPI; it does not rebuild them in the publishing job.
7. Verify installation of the exact released version from normal PyPI.

TestPyPI and PyPI track versions independently, so `0.1.0` can be rehearsed on
TestPyPI and then published on PyPI. Uploaded filenames cannot be overwritten;
use a new version for changed artifacts. Keep the tagged source identical to
the source rehearsed on TestPyPI. Both workflows build a wheel and source archive.

Local packaging checks before release:

```sh
uv sync --locked
uv run ruff check .
uv run pytest -q
uv build
uvx twine check --strict dist/*
```

Use a clean checkout or an empty output directory when building release artifacts,
so files from older versions are not included in an upload. Integration tests
remain an explicit separate step; the release workflow has no Jetstream API key.
