# Globus

Many scientific datasets stored on secure HPC systems and servers are accessible
via the [Globus](https://www.globus.org/) file transfer service.
Globus uses a proprietary file transfer protocol, making it somewhat challenging
to integrate with an open-data system like Pangeo Forge.
In general, Globus file transfers require interaction with the Globus APIs,
which requires accounts and authentication.
Full support for Globus is on our roadmap. In the meantime, we recommend the
following workaround.

To ingest files from Globus with Pangeo Forge, you must create a
_fully public Guest Collection and access the files via HTTPS_.
The specific steps are as follows:
- Verify that the Globus endpoint you want to transfer from uses
  **Globus Connect Server V5**. This method _will not work_ with earlier Globus
  versions (e.g. V4).
- Open the [Globus App](https://app) and use the File Manager to navigate to
  the directory you want to share.
- Create a Public Guest Collection. Follow the Globus HOWTO instructions for
  - [Creating a Guest Collection](https://docs.globus.org/how-to/share-files/)
  - [Share data from a Guest Collection](https://docs.globus.org/how-to/guest-collection-share-and-access/).
    Under "Share With", you need to choose _public (anonymous) - make data accessible to everyone._
- Your data are now accessible via HTTPS. However, you need to figure out the URL.
  The easiest way to do this is via the Globus App. Right click on a specific file
  (not a directory!) within your Guest Collection and click "Get Link", then
  choose the option for "download the file directly". It should show an HTTPS link.
  Based on this link, you should be able to infer the relevant pattern for all
  your URLs.
  (For more details on HTTPS URLs, review the Globus docs on
  [HTTPS Access to Collections](https://docs.globus.org/globus-connect-server/v5/https-access-collections/).)

From here on, you should be able to point to your Globus files using standard
HTTPS URLs. You can proceed to create a File Pattern for your recipe.
