# Pangeo Forge

Pangeo Forge is an open source framework for data Extraction, Transformation, and Loading (ETL) of
scientific data. The goal of Pangeo Forge is to make it easy to extract data from traditional data
archives and deposit in cloud object storage in [analysis-ready, cloud-optimized (ARCO)](https://ieeexplore.ieee.org/abstract/document/9354557) formats.

Pangeo Forge is made of three official components:

- `pangeo-forge-recipes` **Python SDK** - A Python toolkit, built on
  [Apache Beam](https://beam.apache.org), that provides components for concisely and expressively
  composing parallelizable ETL pipelines ("recipes") that can be run on your local machine
  or cloud infrastructure. See {doc}`composition/index` for more on composing recipes,
  and {doc}`deployment/index` for ways to run them.
- `pangeo-forge-runner` **Command Line Interface (CLI)** - A utility for managing the configuration
  and deployment of version-controlled recipes. See {doc}`deployment/cli` for details.
- **Deploy Recipe Action** - A Github Action that wraps the CLI, providing an interface
for configuration and deployment of recipes in response to to GitHub Event triggers.
See {doc}`deployment/action` for details.

A growing {doc}`ecosystem` of third-party extensions provide additional reusable components for
customization of recipes.

Pangeo Forge is inspired directly by [Conda Forge](https://conda-forge.org/), a community-led
collection of recipes for building conda packages. We hope that Pangeo Forge can play a similar
role for datasets.

## The Pangeo Forge Paper

We wrote [a long academic paper about Pangeo Forge](https://doi.org/10.3389/fclim.2021.782909).
This is the best thing to read if you want a deep dive into why we created Pangeo Forge.
If you use Pangeo Forge in academic work, please cite this paper.

````{admonition} Bibtex entry for Pangeo Forge paper
:class: dropdown

```text
@ARTICLE{10.3389/fclim.2021.782909,
AUTHOR={Stern, Charles and Abernathey, Ryan and Hamman, Joseph and Wegener, Rachel and Lepore, Chiara and Harkins, Sean and Merose, Alexander},
TITLE={Pangeo Forge: Crowdsourcing Analysis-Ready, Cloud Optimized Data Production},
JOURNAL={Frontiers in Climate},
VOLUME={3},
YEAR={2022},
URL={https://www.frontiersin.org/article/10.3389/fclim.2021.782909},
DOI={10.3389/fclim.2021.782909},
ISSN={2624-9553},
}
```

````

```{note}
As noted in the paper, _"Pangeo Forge follows an agile development model, characterized by rapid
iteration, frequent releases, and continuous feedback from users."_ Readers should therefore be
mindful that while core principles including reproducibility, collaboration, and provenance
transparency remain consistent over time, implementation details and project scope have evolved
since publication.
```


## Frequently Asked Questions

### Is Pangeo Forge the right tool for my dataset?

Pangeo Forge is currently focused primarily on multidimensional array data;
i.e., data that fit the NetCDF data model and can be opened by Xarray.
This includes NetCDF, GRIB, HDF4/5, Zarr, OPeNDAP, and many other obscure formats.
Pangeo Forge is _not_ currently focused on tabular data;
i.e., data that can be opened by Pandas, CSV, SQL, Parquet, Arrow, etc. This data model and
these formats are very well supported by other ETL tools in the modern data stack.

So if you want to do ETL on multidimensional array data, Pangeo Forge is for you!

### How is Pangeo Forge funded?

Right now, the development of Pangeo Forge is funded exclusively by the US National Science
Foundation (NSF)
EarthCube program, via award [2026932](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2026932)
to Lamont Doherty Earth Observatory. The lead PI of the award is Ryan Abernathey.

Going forward, we hope to establish a multi-stakeholder coalition to support
the operation of Pangeo Forge as a stable, reliable service for the research community.
Please reach out if you're interested in helping with this effort.

### How can I help with development?

Check out our {doc}`development` for how to get started!

## Connecting with the Community

Pangeo Forge is a community run effort with a variety of roles:

- **Recipe developers** — Data engineers and enthusiasts who write recipes to define the data conversions.
This can be anyone with a desire to create analysis ready cloud-optimized (ARCO) data. Explore
{doc}`composition/index` for more on this role.
- **Data users** - Analysts, scientists, and domain experts who use the ARCO data produced by Pangeo Forge
in their work and research.
- **Tooling developers** - Scientists and software developers who maintain and enhance the
open-source code base which makes Pangeo Forge run. See {doc}`development` for more.


## Site Contents

```{toctree}
:maxdepth: 2

self
getting_started/index
composition/index
deployment/index
advanced/index
ecosystem
api_reference
development
release_notes
```
