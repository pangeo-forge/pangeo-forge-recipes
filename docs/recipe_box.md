# The Recipe Box

You are free to use Pangeo Forge Recipes in private, {doc}`executing <execute>`
them on your own computers / infrastructure however you wish.
However, the real point of Pangeo Forge is to create a crowdsourced database
of public recipes, which are executed automatically in the cloud via {doc}`bakeries`.
This database is called the Pangeo Forge **Recipe Box**.

```{note}
The Recipe Box is under heavy development, and many details are still being worked out.
The process of contributing a recipe will likely evolve and change over time.
```

### Contributing a Recipe

To contribute a recipe, you will need to understand the basics of Pangeo Forge {doc}`recipes`.
Working through the {doc}`tutorials/index` is the best way to develop your understanding.
You will also need a thorough understanding of the source dataset you wish to ingest,
and a vision for the desired target dataset (desired format, chunk structure, etc.)
and any cleaning / processing steps needed along the way.

Once you are ready to begin, head over to <https://github.com/pangeo-forge/staged-recipes>.
This repo is used to triage all new recipes for including in the Recipe Box.
The basic steps for adding you recipe are

1. [fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) the <https://github.com/pangeo-forge/staged-recipes> GitHub repository
2. Add your recipe(s) to a new folder.
3. Submit the new recipe(s) as a pull request

At this point the pangeo-forge maintainers / bots will verify that your recipe
is shipshape and ready for inclusion in the Recipe Box.
