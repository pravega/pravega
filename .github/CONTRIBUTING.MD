Guidelines for Issues and Pull Request
======================================

Note: This guideline is inspired by
[Docker](https://github.com/docker/docker/blob/master/project/REVIEWING.md) and
its extension projects such as swarm. 

Motivation
----------

Once we adapt guideline, it will help us

-   To keep Pravega development manageable and organized as project scales in
    terms of complexity and community

-   To have clear visualization on various pipelines, whether it is for bug,
    area, feature, sprint, release or individual contributor

-   To plan more accurately by avoiding both under and over loading so we
    could set better expectations for business stakeholders

-   To easily track progresses and milestones

Guideline Proposal
------------------

### Issues

An issue can have multiple of the following labels. Typically, a properly
classified issue should have:

1.  *One label* identifying its kind (kind/\*).

2.  One or multiple *labels* identifying the functional areas of interest
    (area/\*)

3.  Where applicable, *one label* categorizing its difficulty (exp/\*)

4.  Where applicable, associate issue to a milestone, i.e. sprint

**Issue kind**

| **Kind **        | **Description**                                                                                                              |
|------------------|------------------------------------------------------------------------------------------------------------------------------|
| kind/bug         | Bugs are bugs. The cause may or may not be known at triage time so debugging should be taken account into the time estimate. |
| kind/enhancement | Enhancements are not bugs or new features but can drastically improve usability or performance of a project component.       |
| kind/feature     | Functionality or other elements that the project does not currently support. Features are new and shiny.                     |
| kind/question    | Contains a user or contributor question requiring a response.                                                                |

  
**Functional area**

| **Area**         |
|------------------|
| area/api         |
| area/build       |
| area/controller  |
| area/server      |
| area/client      |
| area/docs        |
| area/tier1       |
| area/tier2       |
| area/performance |
| area/security    |
| area/logging    |
| area/testing     |

**Experience level**

Experience level is a way for a contributor to find an issue based on their
skill set. Experience types are applied to the issue or pull request using
labels.

| **Level**        | **Experience level guideline**                                                                                                                                    |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| exp/beginner     | New to Pravega, and is looking to help while learning the basics.                                                                                                 |
| exp/intermediate | Comfortable with Java and understands the core concepts of Pravega and looking to dive deeper into the project.                                                   |
| exp/expert       | Proficient with Pravega and has been following, and active in, the community to understand the rationale behind design decisions and where the project is headed. |

**Issue status**

To communicate the status with other collaborators, you can apply status labels
to issues. These labels prevent duplicating effort.

| **Status**              | **Description**                                                                                                                                                                                                                                                    |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| no status               | Issue has been created, not yet validated by reproducing claimed bug or not yet accepted due to lack of justification                                                                                                                                              |
| status/accepted         | Apply to enhancements / feature requests that we think are good to have. Adding this label helps contributors find things to work on. Serves as a backlog.                                                                                                         |
| status/more-info-needed | Apply this to issues that are missing information (eventually we need to have Issue Template, that requires Pravega Version, Runtime environment ..), or require feedback from the reporter. If the issue is not updated after a week, it can generally be closed. |
| status/in-progress      | Apply this label if an issue has been picked up by a developer and in design discussion/development                                                                                                                                                                |
| status/needs-attention  | Apply this label if an issue (or PR) needs more eyes.                                                                                                                                                                                                              |

  
**Issue Estimation**

To plan more accurately and manage expectations of other collaborators, you can
apply estimation labels to issues. These labels will us estimate and track
overall sprint progress, as well as avoid a developer being overloaded (or
underloaded) with more than what they could accomplish in a sprint.

 

| **Estimation**             | **Description**                                                                                                                                                      |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| est/1, 3, 5, 8, 13, 21, 40 | Apply these labels to a issue based on how many hours it will take to complete. If an issue takes more than 40 hours, breaking down into manageable pieces will work |

**Issue Workflows**

Although there could be multiple variations in issue transitions, here is a
possible and the most common workflow. 

![C:\\a21d64e7e1a8dfc46a7adad79585fbf2](media/268405eb0263ec70881ae43aa7c02dab.png)

### Pull Request Reviews and Labeling

**Status labels**

| **Status**              | **Description**                                                                                                                                                                                                       |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| status/1-design-review  | Maintainers are expected to comment on the design of the pull request. Review of documentation is expected only in the context of design validation, not for stylistic changes. Possible transitions from this state: |
| status/2-code-review    | Maintainers are expected to review the code and ensure that it is good quality and in accordance with the documentation in the PR. Possible transitions from this state:                                              |
| status/3-docs-review    | Maintainers are expected to review the documentation in its bigger context, ensuring consistency, completeness, validity, and breadth of coverage across all existing and new documentation.                          |
| status/4-ready-to-merge | Maintainers are expected to merge this pull request as soon as possible. They can ask for a rebase or carry the pull request themselves. Possible transitions from this state:                                        |
| closed                  | If a pull request is closed it is expected that sufficient justification will be provided.                                                                                                                            |

-   Close: design rejected

-   status/2-code-review: general case

-   status/3-docs-review: proposals with only documentation changes

-   Close

-   status/1-design-review: new design concerns are raised

-   status/3-docs-review: general case

-   status/4-ready-to-merge: change not impacting documentation

-   If the pull request does not impact the documentation at all, the docs
    review step is skipped, and the pull request is ready to merge
 
