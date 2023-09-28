# Contributing

👋 Welcome!
If you're interested in contributing to Pangeo Forge, this is the place to start.

## Reporting Bugs and Requesting Features

You don't have to do any coding to contribute.
Reporting bugs and requesting new features is a very valuable contribution.
To report bugs or request new features, head over to the relevant issue page:

- <https://github.com/pangeo-forge/pangeo-forge-recipes/issues>
- <https://github.com/pangeo-forge/pangeo-forge-runner/issues>
- <https://github.com/pangeo-forge/deploy-recipe-action/issues>

## Dev environment setup

### Fork and clone the repo

To contribute, first fork the repository on GitHub: <https://github.com/pangeo-forge/pangeo-forge-recipes>.
To checkout your fork locally and create a remote pointing to the upstream repo,
run the following commands from the command line:

```bash
git clone git@github.com:{{ your_username }}/pangeo-forge-recipes.git
cd pangeo-forge-recipes
git remote add upstream git@github.com:pangeo-forge/pangeo-forge-recipes.git
```

From now on, we will assume all shell commands are from within the
`pangeo-forge-recipes` directory.

### Create a virtual environment

We strongly recommend creating an isolated
[Conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
from which to develop and test your code.
This ensures you have all the correct dependencies and tools involved.

https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands

### Set up pre-commit

We use [pre-commit](https://pre-commit.com/) to manage code linting and style.
To set up pre-commit:

```bash
pip install pre-commit
pre-commit install
```

## Development lifecycle

[![GitHub pull requests](https://img.shields.io/github/issues-pr/pangeo-forge/pangeo-forge-recipes?style=flat-square)](https://github.com/pangeo-forge/pangeo-forge-recipes/pulls)

When you are ready to make changes.

### Create a new feature branch off `main`:

```bash
git checkout -b my-cool-feature
```

### Commit changes

Make changes using your favorite text editor.

```bash
git add path/to/changed_files
git commit -m 'informative commit message'
```

### Push changes to GitHub

```bash
git push origin my-cool-feature
```

### Open a PR

[Pull Request](https://github.com/pangeo-forge/pangeo-forge-recipes/pulls)

- Make more changes in response to PR review
- Push more commits to your remote branch

### Cleanup

Once your PR is merged:
```bash
git checkout main
git fetch upstream
git rebase upstream/main
# clean up feature branch
git branch -d my-cool-feature
```

## Contributing: docs

We strongly encourage contributions to make the documentation clearer and more complete.

### Install dependencies

To build the documentation, you will need the additional requirements located at
`docs/requirements.txt`.
Assuming you are already inside the `pangeo-forge-recipes` conda environment, you can run

```bash
pip install -r docs/requirements.txt
```

### Build the docs

To build the documentation
```bash
cd docs
make html
```

### View the docs

Serving the documentation locally (starting from the `docs` directory)
```bash
cd _build/html; python -m http.server; cd ../..
# press ctrl-C to exit
```

### Make changes

...

## Contributing: code

If you're ready to start contributing to the code, here is where to start.

### Install dependencies

### Run the test suite

The tests are as important as the code itself--they verify that pangeo-forge-recipes
actually works.

To run the test suite:

```bash
pytest tests -v
```

All of the tests
## Releasing

To make a new release, just go to <https://github.com/pangeo-forge/pangeo-forge-recipes/releases>
and click "Draft a new release".
The [release.yaml](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/master/.github/workflows/release.yaml) -
workflow should take care of the rest.
