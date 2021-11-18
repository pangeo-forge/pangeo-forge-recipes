# Development Guide

👋 Welcome!
If you're interested in contributing to Pangeo Forge, this is the place to start.

## Reporting Bugs and Requesting Features

You don't have to do any coding to contribute.
Reporting bugs and requesting new features is a very valuable contribution.

To report bugs or request new features, head over to
<https://github.com/pangeo-forge/pangeo-forge-recipes/issues>.

![GitHub issues](https://img.shields.io/github/issues/pangeo-forge/pangeo-forge-recipes?style=flat-square)

## Contributing to the Code

If you're ready to start contributing to the code, here is where to start.

### Fork and Clone the Repo

The project lives on GitHub at
<https://github.com/pangeo-forge/pangeo-forge-recipes/issues>.

To contribute, first fork the repository on GitHub.
You will then have your own clone in GitHub.
To check this out locally and also create a remote pointing to the upstream repo,
run the following commands from the command line.

```bash
git clone git@github.com:{{ your_username }}/pangeo-forge-recipes.git
cd pangeo-forge-recipes
git remote add upstream git@github.com:pangeo-forge/pangeo-forge-recipes.git
```

From now on, we will assume all shell commands are from within the
`pangeo-forge-recipes` directory.

### Repository Layout

Here is what you will find in the repository

- _Top level_ - configuration files, `setup.py`, etc.
- `.github/workflows` - Github Worflows
- `ci` - Environment specifications
- `docs` - Sphinx Documentation
- `pangeo_forge_recipes` - The main python package
- `tests` - Pytest test suite

### Create a Development Environment

We strongly recommend creating an isolated
[Conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
from which to develop and test your code.
This ensures you have all the correct dependencies and tools involved.

The recommended development environment is located in `ci/py3.9.yml`.
To create and activate this environment, do

```bash
conda env create --file ci/py3.9.yml
conda activate pangeo-forge-recipes
```

### Set up pre-commit

We use [pre-commit](https://pre-commit.com/) to manage code linting and style.
To set up pre-commit:

```bash
pip install pre-commit
pre-commit install
```

### Run the Test Suite

The tests are as important as the code itself--they verify that pangeo-forge-recipes
actually works.

To run the test suite:

```bash
py.test tests -v
```

There are a couple of extra options you can enable to see more output.

The following will get you all of the logs:

```bash
py.test -v tests --log-cli-level=DEBUG
````

The following will get dask worker logs redirected to STDOUT:

```bash
py.test tests -v --redirect-dask-worker-logs-to-stdout=DEBUG
```

These can be useful in debugging.

#### Custom Markers

The test suite is configured with a custom set of [pytest markers](https://docs.pytest.org/en/latest/example/markers.htm).
These can be used to select specific executors to test.
This feature is used in the [main test Github Workfow](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/main.yaml).
These marks are enumerated in [setup.cfg](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/setup.cfg).

For example, to run just the test that use the Dask executor, you can do

```bash
py.test tests -m executor_dask
```

### Submit a Pull Request

[![GitHub pull requests](https://img.shields.io/github/issues-pr/pangeo-forge/pangeo-forge-recipes?style=flat-square)](https://github.com/pangeo-forge/pangeo-forge-recipes/pulls)

When you are ready to make changes.

- Create a new feature branch off `master`:
   ```bash
   git checkout -b my-cool-feature
   ```
- Make your changes using your favorite text editor. Then
  ```bash
  git add path/to/changed_files
  git commit -m 'informative commit message'
  ```
- Push your branch to github
  ```bash
  git push origin my-cool-feature
  ```
- Open a [Pull Request](https://github.com/pangeo-forge/pangeo-forge-recipes/pulls)
- Make more changes in response to PR review
- Push more commits to your remote branch
- Once your PR is merged:
  ```bash
  git checkout master
  git fetch upsream
  git rebase upstream/master
  # clean up feature branch
  git branch -d my-cool-feature
  ```

### Continuous Integration

Continuous integration is done with [GitHub Actions](https://docs.github.com/en/actions/learn-github-actions).

There are currently 5 workflows configured:

- [pre-commit.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/pre-commit.yaml) -
  Code linting.
- [main.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/main.yaml) -
  Run test suite with pytest.
- [slash-command-dispatch.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/slash-command-dispatch.yaml) and [tutorials.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/tutorials.yaml) -
  These workflows collaborate to enable the slash-command `/run-test-tutorials` to be run
  via a comment on a PR. This enables us to check whether the PR breaks any of our tutorials.
  We don't run the tutorials as part of regular CI because they are very slow.
  (However, the tutorial workflow _is_ run on commits to the `master` branch.)
- [release.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/release.yaml) -
  This workflow generates a pypi release every time a GitHub release is created.

## Contributing to the Documentation

We strongly encourage contributions to make the documentation clearer and more complete.

To build the documentation, you will need the additional requirements located at
`docs/requirements.txt`.
Assuming you are already inside the `pangeo-forge-recipes` conda environment, you can run

```bash
pip install -r docs/requirements.txt
```

To build the documentation
```bash
cd docs
make html
```

Serving the documentation locally (staring from the `docs` directory)
```bash
cd _build/html; python -m http.server; cd ../..
# press ctrl-C to exit
```

## Releasing

To make a new release, just go to <https://github.com/pangeo-forge/pangeo-forge-recipes/releases>
and click "Draft a new release".
The [release.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/release.yaml) -
workflow should take care of the rest.
