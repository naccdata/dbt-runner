# {{gear_name}} ({{gear_label}})

## Overview

*[Usage](#usage)*

*[FAQ](#faq)*

### Summary

*{From the "description" section of the manifest}*

### Cite

*{From the "cite" section of the manifest}*

### License

*License:* *{From the "license" section of the manifest. Be as specific as possible, specifically when marked as Other.}*

### Classification

*Category:* *{From the "custom.gear-builder.category" section of the manifest}*

*Gear Level:*

- [ ] Project
- [ ] Subject
- [ ] Session
- [ ] Acquisition
- [ ] Analysis

----

[[_TOC_]]

----

### Inputs

- *{Input-File}*
  - __Name__: *{From "inputs.Input-File"}*
  - __Type__: *{From "inputs.Input-File.base"}*
  - __Optional__: *{From "inputs.Input-File.optional"}*
  - __Classification__: *{Based on "inputs.Input-File.base"}*
  - __Description__: *{From "inputs.Input-File.description"}*
  - __Notes__: *{Any additional notes to be provided by the user}*

### Config

- *{Config-Option}*
  - __Name__: *{From "config.Config-Option"}*
  - __Type__: *{From "config.Config-Option.type"}*
  - __Description__: *{From "config.Config-Option.description"}*
  - __Default__: *{From "config.Config-Option.default"}*

### Outputs

#### Files

*{A list of output files (if possible?)}*

- *{Output-File}*
  - __Name__: *{From "outputs.Input-File"}*
  - __Type__: *{From "outputs.Input-File.base"}*
  - __Optional__: *{From "outputs.Input-File.optional"}*
  - __Classification__: *{Based on "outputs.Input-File.base"}*
  - __Description__: *{From "outputs.Input-File.description"}*
  - __Notes__: *{Any additional notes to be provided by the user}*

#### Metadata

Any notes on metadata created by this gear

### Pre-requisites

This section contains any prerequisites

#### Prerequisite Gear Runs

A list of gears, in the order they need to be run:

1. __*{Gear-Name}__*
    - Level: *{Level at which gear needs to be run}*

#### Prerequisite Files

A list of any files (OTHER than those specified by the input) that the gear will need.
If possible, list as many specific files as you can:

1. ____{File-Name}__*
    - Origin: *{Gear-Name, or Scanner, or Upload?}*
    - Level: *{Container level the file is at}*
    - Classification: *{Required classification(s) that the file can be}*

#### Prerequisite Metadata

A description of any metadata that is needed for the gear to run.
If possible, list as many specific metadata objects that are required:

1. __*{Metadata-Key}__*
    - Location: *{Nested Metadata Location (info.object1, age, etc)}*
    - Level: *{Container level that metadata is at}*

## Usage

This section provides a more detailed description of the gear, including not just WHAT it does, but HOW it works in flywheel.

### Description

*{A detailed description of how the gear works}*

#### File Specifications

This section contains specifications on any input files that the gear may need

##### *{Input-File}*

A description of the input file

### Workflow

A picture and description of the workflow

```mermaid
graph LR;
    A[Input-File]:::input --> C;
    C[Upload] --> D[Parent Container <br> Project, Subject, etc];
    D:::container --> E((Gear));
    E:::gear --> F[Analysis]:::container;

    classDef container fill:#57d,color:#fff
    classDef input fill:#7a9,color:#fff
    classDef gear fill:#659,color:#fff

```

Description of workflow

1. Upload file to container
1. Select file as input to gear
1. Geat places output in Analysis

### Use Cases

This section is very gear dependent, and covers a detailed walkthrough of some use cases.  Should include Screenshots, example files, etc.

#### Use Case 1

__*Conditions__*:

- *{A list of conditions that result in this use case}*
- [ ] Possibly a list of check boxes indicating things that are absent
- [x] and things that are present

*{Description of the use case}*

### Logging

An overview/orientation of the logging and how to interpret it.

## FAQ

[FAQ.md](FAQ.md)

## Contributing

[For more information about how to get started contributing to that gear,
checkout [CONTRIBUTING.md](CONTRIBUTING.md).]
<!-- markdownlint-disable-file -->
