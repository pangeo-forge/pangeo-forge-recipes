# Contributing

## Open an issue

You don't have to do any coding to contribute.
Reporting bugs and requesting new features via GitHub Issues is a very valuable contribution.
To open a new issue, head over to the relevant issue page:

- <https://github.com/pangeo-forge/pangeo-forge-recipes/issues>:
For anything related to {doc}`composition/index`.
- <https://github.com/pangeo-forge/pangeo-forge-runner/issues>:
For the {doc}`deployment/cli`.
- <https://github.com/pangeo-forge/deploy-recipe-action/issues>:
For the {doc}`deployment/action`.

## Dev environment setup

If you plan to contribute [docs](#contributing-docs) or [code](#contributing-code),
you will need a local development environment.

### Fork and clone the repo

First, fork the repository on GitHub: <https://github.com/pangeo-forge/pangeo-forge-recipes>.
Then, checkout your fork locally:

```bash
git clone git@github.com:{{ your_username }}/pangeo-forge-recipes.git
cd pangeo-forge-recipes
```

```{note}
From now on, we will assume all shell commands are from within the
`pangeo-forge-recipes` directory.
```

Finally, create a new git remote pointing to the upstream repo:

```bash
git remote add upstream git@github.com:pangeo-forge/pangeo-forge-recipes.git
```


### Create a virtual environment

We strongly recommend creating an isolated virtual environment,
which ensures you have all the correct dependencies and tools for development.

This can be either a
[Conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
or a Python [`venv`](https://docs.python.org/3/library/venv.html).

### Set up pre-commit

We use [pre-commit](https://pre-commit.com/) to manage code linting and style.
Once you have a [virtual environment](#create-a-virtual-environment) created and activated,
run the following commands (from the repo root) to setup pre-commit:

```bash
pip install pre-commit
pre-commit install
```

## Development lifecycle

### Create a new branch

When you are ready to make changes, start by creating a new branch from `main`:

```bash
git checkout -b my-cool-feature
```

### Commit changes

Make changes using your favorite text editor, and commit them as you go along:

```bash
git add path/to/changed_files
git commit -m 'informative commit message'
```

### Open a PR

When you are ready for feedback on your changes, push them to GitHub:

```bash
git push origin my-cool-feature
```

And then open a [Pull Request](https://github.com/pangeo-forge/pangeo-forge-recipes/pulls)
against `main` with your changes.

### Cleanup

Once your PR is merged, rebase your local `main` to match changes upstream:

```bash
git checkout main
git fetch upstream
git rebase upstream/main
```

And delete the feature branch:

```bash
git branch -d my-cool-feature
```

## Contributing: docs

We strongly encourage contributions to make the documentation clearer and more complete.

### Install dependencies

With your [virtual environment](#create-a-virtual-environment) activated, run:

```bash
pip install -r docs/requirements.txt
```

### Build the docs

To build the documentation:

```bash
cd docs
make html
```

### View the docs

Serving the documentation locally (starting from the `docs` directory):

```bash
cd _build/html; python -m http.server; cd ../..
# press ctrl-C to exit
```

Alternatively you can open `docs/_build/html/index.html` in your web browser.

## Contributing: code

If you're ready to start contributing to the code, here is where to start.

### Install dependencies

With your [virtual environment](#create-a-virtual-environment) activated, run:

```bash
pip install -e ".[dev]"
```

### Run the test suite

The tests are as important as the code itself.
They verify that `pangeo-forge-recipes` actually works.

To run the test suite:

```bash
pytest tests -v
```

## Releasing

Navigate to <https://github.com/pangeo-forge/pangeo-forge-recipes/releases> and click "Draft a new release".

![How to release gif](https://github.com/pangeo-forge/pangeo-forge-recipes/assets/15016780/c6132967-4f6d-49d9-96eb-48a687130f97)

The [release.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/main/.github/workflows/release.yaml) will be trigged and publish the new version of `pangeo-forge-recipes` to pypi.
