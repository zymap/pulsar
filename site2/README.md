<<<<<<< HEAD


# The Pulsar website and documentation

This `README` is basically the meta-documentation for the Pulsar website and documentation. Here you'll find instructions on running the site locally

## Tools

Framework [Docusaurus](https://docusaurus.io/).

Ensure you have the latest version of [Node](https://nodejs.org/en/download/) installed. We also recommend you install [Yarn](https://yarnpkg.com/en/docs/install) as well.

> You have to be on Node >= 8.x and Yarn >= 1.5.


## Running the site locally

To run the site locally:

```bash
cd website
yarn install
yarn start
```

Note that the `/docs/en/` path shows the documentation for the latest stable release of Pulsar.  Change it to `/docs/en/next/` to show your local changes, with live refresh.

## Contribute

The website is comprised of two parts, one is documentation, while the other is website pages (including blog posts).

Documentation related pages are placed under `docs` directory. They are written in [Markdown](http://daringfireball.net/projects/markdown/syntax).
All documentation pages are versioned. See more details in [versioning](#versioning) section.

Website pages are non-versioned. They are placed under `website` directory.

### Documentation

#### Layout

All the markdown files are placed under `docs` directory. It is a flat structure.
=======
# Pulsar website and documentation

Pulsar website is comprised of two parts: website pages (including blog posts) and documentation.

You can run the website locally to test your updates. The documentation is written in English, and we also encourage contributions in different languages.


## Website

Pulsar website framework adopts [Docusaurus](https://docusaurus.io/). Website pages are non-versioned. They are placed in the `/site2/website` directory. Ensure that you have installed the latest version of [Node](https://nodejs.org/en/download/) and [Yarn](https://yarnpkg.com/en/docs/install) before running the site locally.

> You have to be on Node >= 8.x and Yarn >= 1.5.

### Run the site locally

To run the site locally, enter the following commands.

```bash 
git clone https://github.com/apache/pulsar.git
cd pulsar/site2/website
yarn install
yarn start
```
> Notes
> 
> 1. If you have installed `yarn`, you can skip the `yarn install` command.
> 2. After you enter the `yarn start` command, you will be navigated to a local address, for example, `http://localhost:3000`. Click `Docs` to see documentation for the latest release of Pulsar. 
> 3. The `http://localhost:3000/en/versions` path shows the documentation for all versions. To view your local changes, click `Documentation` in **Latest Version**, or enter `http://localhost:3000/docs/en/next/standalone` in a browser.

### Tests

To run tests locally, enter the following commands
```bash
cd pulsar/site2/website
yarn test
```

### Check

Before submitting a pull request, run the following command to make sure no broken links exist.

```
cd pulsar/site2/website
yarn build
```

If warning messages are shown as below, it means broken links exist.

```
[WARN] unresolved links in file 'version-2.4.1/getting-started-standalone.md' > [ 'security-overview.md' ]

[WARN] unresolved links in file 'version-2.4.0/io-managing.md' > [ 'functions-overview.md' ]
```

Fix the broken links manually and then send a pull request.

## Documentation
Pulsar documents are written in English. Documentation related pages are placed in the `/site2/docs` directory. All documentation pages are versioned. For more details, refer to [versioning](#versioning).

### Contribute to documentation

We welcome contributions to help improve Pulsar documentation. The documents are written in [Markdown](http://daringfireball.net/projects/markdown/syntax) and follow [Google Developer Documentation Style Guide](https://developers.google.com/style/). If you are not familiar with the writing styles, we are happy to guide you along the way.

For workflow on how to contribute to Pulsar, refer to [contribution](http://pulsar.apache.org/en/contributing/) guidelines.

To learn more about Pulsar documents, read the following instructions.

### Layout

The markdown files placed in the `docs` directory adopt a flat structure.
>>>>>>> f773c602c... Test pr 10 (#27)

```
├── docs
│   ├── adaptors-kafka.md
│   ├── adaptors-spark.md
│   ├── adaptors-storm.md
│   ├── admin-api-brokers.md
│   ├── admin-api-clusters.md
│   ├── admin-api-namespaces.md
│   ├── admin-api-non-persistent-topics.md
│   ├── admin-api-overview.md
│   ├── admin-api-partitioned-topics.md
│   ├── admin-api-permissions.md
│   ├── admin-api-persistent-topics.md
│   ├── admin-api-tenants.md
│   ├── administration-dashboard.md
│   ├── administration-geo.md
│   ├── administration-load-distribution.md
│   ├── administration-proxy.md
...
```

All the files are named in the following convention:

```
<category>-<page-name>.md
```

<<<<<<< HEAD
`<category>` is the category within the sidebard that this file belongs to, while `<page-name>` is the string to name the file within this category.

There isn't any constraints on how files are named. It is just a naming convention for better maintenance.

#### Document

##### Markdown Headers

All the documents are the usual Markdown files. However you need to add some Docusaurus-specific fields in Markdown headers in order to link them
correctly to the [Sidebar](#sidebar) and [Navigation Bar](#navigation).

`id`: A unique document id. If this field is not present, the document's id will default to its file name (without the extension).

`title`: The title of your document. If this field is not present, the document's title will default to its id.

`hide_title`: Whether to hide the title at the top of the doc.

`sidebar_label`: The text shown in the document sidebar for this document. If this field is not present, the document's `sidebar_label` will default to its title.
=======
`<category>` is the category within the sidebar that this file belongs to, while `<page-name>` is the string to name the file within this category.

### Markdown Headers

All the documents are usual Markdown files. However you need to add some Docusaurus-specific fields in Markdown headers in order to link them correctly to the [Sidebar](#sidebar) and [Navigation Bar](#navigation).

`id`: A unique document ID. If this field is not specified, the document ID defaults to its file name (without the extension).

`title`: The title of the document. If this field is not specified, the document title defaults to its id.

`hide_title`: Whether to hide the title at the top of the doc.

`sidebar_label`: The text shown in the document sidebar for this document. If this field is not specified, the document `sidebar_label` defaults to its title.
>>>>>>> f773c602c... Test pr 10 (#27)

For example:

```bash
---
id: io-overview
title: Pulsar IO Overview
sidebar_label: Overview
---
```

<<<<<<< HEAD
##### Linking to another document

You can use relative URLs to other documentation files which will automatically get converted to the corresponding HTML links when they get rendered.
=======
### Link to another document

To link to other documentation files, you can use relative URLs, which will be automatically converted to the corresponding HTML links when they are rendered.
>>>>>>> f773c602c... Test pr 10 (#27)

Example:

```md
[This links to another document](other-document.md)
```

<<<<<<< HEAD
This markdown will automatically get converted into a link to /docs/other-document.html (or the appropriately translated/versioned link) once it gets rendered.

This can help when you want to navigate through docs on GitHub since the links there will be functional links to other documents (still on GitHub),
but the documents will have the correct HTML links when they get rendered.

#### Linking to javadoc of pulsar class

We have a [remarkable plugin](https://github.com/jonschlinkert/remarkable) for generating links to the javadoc for pulsar classes.
You can write them in following syntax:
=======
The markdown file will be automatically converted into a link to /docs/other-document.html (or the appropriately translated/versioned link) once it is rendered.

This helps when you want to navigate through docs on GitHub since the links there are functional links to other documents (still on GitHub), and the documents have the correct HTML links when they are rendered.

### Link to javadoc of Pulsar class

We have a [remarkable plugin](https://github.com/jonschlinkert/remarkable) to generate links to the javadoc for Pulsar classes.
You can write them in the following syntax:
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
{@inject: javadoc:<Display Name>:<Relative-Path-To-Javadoc-Html-File>}
```

<<<<<<< HEAD
For example, following line generates a hyperlink to the javadoc of `PulsarAdmin` class.
=======
For example, the following line generates a hyperlink to the javadoc of `PulsarAdmin` class.
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
{@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin.html}
```

<<<<<<< HEAD
#### Linking to files in Pulsar github repo

We are using same remarkable plugin to generate links to the files in Pulsar github repo.
=======
### Link to files in Pulsar GitHub repository

We use the same [remarkable plugin](https://github.com/jonschlinkert/remarkable) to generate links to files in Pulsar GitHub repository.
>>>>>>> f773c602c... Test pr 10 (#27)

You can write it using similar syntax:

```shell
{@inject: github:<Display Text>:<Relative-Path-To-Files>}
```

<<<<<<< HEAD
For example, following line generates a hyperlink to the dashboard Dockerfile.
=======
For example, the following line generates a hyperlink to the dashboard Dockerfile.
>>>>>>> f773c602c... Test pr 10 (#27)

```
{@inject: github:`Dockerfile`:/dashboard/Dockerfile}
```

<<<<<<< HEAD
For more details about markdown features, you can read [here](https://docusaurus.io/docs/en/doc-markdown).

#### Sidebar

All the sidebars are defined in a `sidebars.json` file under `website` directory. The documentation sidebar is named `docs` in that json structure.

When you want to add a page to sidebar, you can add the document `id` you used in the document header to existing sidebar/category. In the blow example,
`docs` is the name of the sidebar, "Getting started" is a category within the sidebar and "pulsar-2.0" is the `id` of one of the documents.
=======
For more details about markdown features, read [here](https://docusaurus.io/docs/en/doc-markdown).

### Sidebar

All the sidebars are defined in a `sidebars.json` file in the `website` directory. The documentation sidebar is named `docs` in the JSON structure.

When you want to add a page to sidebar, you can add the document `id` you used in the document header to the existing sidebar/category. In the example below,
`docs` is the name of the sidebar, "Getting started" is a category within the sidebar, and "pulsar-2.0" is the `id` of a document.
>>>>>>> f773c602c... Test pr 10 (#27)

```bash
{
  "docs": {
    "Getting started": [
      "pulsar-2.0",
      "standalone",
      "standalone-docker",
      "client-libraries",
      "concepts-architecture"
    ],
    ...
  }
}
```

<<<<<<< HEAD
For more details about versioning, you can read [here](https://docusaurus.io/docs/en/navigation).

#### Navigation

To add links to the top navigation bar, you can add entries to the `headerLinks` of `siteConfig.js` under `website` directory.

See [Navigation and Sidebars](https://docusaurus.io/docs/en/navigation) in Docusaurus website to learn different types of links
you can add to the top navigation bar.

## Versioning

Documentation versioning with Docusaurus becomes simpler. When done with a new release, just simply run following command:
=======
### Navigation

To add links to the top navigation bar, you can add entries to the `headerLinks` of `siteConfig.js` under `website` directory.

To learn different types of links you can add to the top navigation bar, refer to [Navigation and Sidebars](https://docusaurus.io/docs/en/navigation).

### Versioning

Documentation versioning with Docusaurus becomes simpler. When done with a new release, just simply run the following command.
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
yarn run version ${version}
```

<<<<<<< HEAD
This will preserve all markdown files currently in `docs` directory and make them available as documentation for version `${version}`.
=======
This preserves all markdown files in the `docs` directory and make them available as documentation for version `${version}`.
>>>>>>> f773c602c... Test pr 10 (#27)
Versioned documents are placed into `website/versioned_docs/version-${version}`, where `${version}` is the version number
you supplied in the command above.

Versioned sidebars are also copied into `website/versioned_sidebars` and are named as `version-${version}-sidebars.json`.

<<<<<<< HEAD
If you wish to change the documentation for a past version, you can access the files for that respective version.

For more details about versioning, you can read [here](https://docusaurus.io/docs/en/versioning).

## Translation and Localization

Docusaurus allows for easy translation functionality using [Crowdin](https://crowdin.com/).
All the markdown files are written in English. These markdown files are uploaded to Crowdin
for translation by the users within a community. Top-level pages are also written in English.
The strings that are needed to be translated are wrapped in a `<translate>` tag.

[Pulsar Website Build](https://builds.apache.org/job/pulsar-website-build/) will automatically
pulling down and uploading translations for all the pulsar website documentation files. Once
it pulls down those translations from Crowdin, it will build those translations into the website.

### Contribute Translations

All the translations are stored and managed in this [Pulsar Crowdin project](https://crowdin.com/project/apache-pulsar).
If you would like to contribute translations, you can simply create a Crowdin account, join the project and make contributions.
Crowdin provides very good documentation for translators. You can read [those documentations](https://support.crowdin.com/crowdin-intro/)
to start contributing.

Your contributed translations will be licensed under [Apache License V2](https://www.apache.org/licenses/LICENSE-2.0).
Pulsar Committers will review those translations. If your translations are not reviewed or approved by any committers,
feel free to reach out to use via [slack channel](https://apache-pulsar.herokuapp.com/) or [mailing lists](https://pulsar.apache.org/contact/).
=======
If you want to change the documentation for a previous version, you can access files for that respective version.

For more details about versioning, refer to [Versioning](https://docusaurus.io/docs/en/versioning).

## Translation and localization

Docusaurus makes it easy to use translation functionality using [Crowdin](https://crowdin.com/).
All the markdown files are written in English. These markdown files are uploaded to Crowdin for translation by users within a community. Top-level pages are also written in English.
The strings that are needed to be translated are wrapped in a `<translate>` tag.

[Pulsar Website Build](https://builds.apache.org/job/pulsar-website-build/) pulls down and uploads translation for all the Pulsar website documentation files automatically. Once it pulls down translation from Crowdin, it will build the translation into the website.

### Contribute translation

Translation is stored and managed in the [Pulsar Crowdin project](https://crowdin.com/project/apache-pulsar).
To contribute translation, you can simply create a Crowdin account, join the project and make contributions.
Crowdin provides very good documentation for translators. You can read [Crowdin Knowledge Base](https://support.crowdin.com/crowdin-intro/) before contributing.

Translation you contribute is licensed under [Apache License V2](https://www.apache.org/licenses/LICENSE-2.0).
Pulsar Committers will review translation. If your translation is not reviewed or approved by any committer, feel free to reach out via [slack channel](https://apache-pulsar.herokuapp.com/) or [mailing lists](https://pulsar.apache.org/contact/).

### Download translated docs

When you find display issues on the translated pages, you can download the translated docs from Crowdin, and follow the instructions below to debug and fix issues.

1. Install Java (optional)
If you have installed Java, skip this step. If you have not installed [Java](https://java.com/en/), install the latest version.
If you are using Mac OS, you can use the following command to install Java:  

```
brew cask install java
```

2. Install Crowdin CLI

To download the translated markdown files, you need to install [Crowdin CLI](https://support.crowdin.com/cli-tool/#installation).

3. Set environment variables

You need to set the following environment variables:

```
export CROWDIN_DOCUSAURUS_PROJECT_ID="apache-pulsar"
export CROWDIN_DOCUSAURUS_API_KEY=<crowdin-pulsar-api-key>
```

You can find the API Key of Pulsar Crowdin project [here](https://crowdin.com/project/apache-pulsar/settings#api). Only PMC members and
committers are able to retrieve the API key. If the API key is invalid, regenerate.

4. Download the translated docs

Now you are ready to download the translated docs from Crowdin.

```
$ cd ${PULSAR_HOME}/site2/website
# download all translated docs
$ yarn crowdin-download
# download the translated docs for `zh-CN`
$ yarn crowdin-download -l zh-CN
```

The translated docs are downloaded to the `site2/website/translated_docs` directory.

### Check issues, fix and verify

After download the translated documents, you can open the target markdown file, check issues and fix them.
To verify if you have fixed the issues correctly, [run the site locally](#run-the-site-locally).
>>>>>>> f773c602c... Test pr 10 (#27)
