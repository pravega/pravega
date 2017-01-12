Guidelines for Issues and Pull Request
======================================

Note: This guideline is inspired by
[Docker](https://github.com/docker/dockercraft/blob/master/CONTRIBUTING.md) and
its extension projects such as swarm. 

Motivation
----------
-   To keep Pravega development manageable and organized as the project scales in
    terms of complexity and community.
-   To have clear visualization on various pipelines, whether it is for a bug,
    area, feature, sprint, release or individual contributor.
-   To easily track progress and milestones.

Guideline Proposal
------------------

### Issues

An issue can have multiple labels of the following types. Typically, a properly
classified issue should have:
1.  *One label* identifying its kind (kind/\*).
2.  One or multiple *labels* identifying the functional areas of interest
    (area/\*).
3.  Where applicable, *one label* categorizing its difficulty (exp/\*).
4.  Where applicable, associate the issue to a milestone, i.e. sprint.

**Issue kind**

| **Kind **        | **Description**                                                                                                              |
|------------------|------------------------------------------------------------------------------------------------------------------------------|
| kind/bug         | Bugs are bugs. The cause may or may not be known at triage time so debugging should be accounted for in the time estimate. |
| kind/enhancement | Enhancements are not bugs or new features but can drastically improve the usability or performance of a project component.       |
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
| exp/beginner     | New to Java, and is looking to help while learning the basics.                                                                                                 |
| exp/intermediate | Comfortable with Java and understands the core concepts of Pravega and looking to dive deeper into the project.                                                   |
| exp/expert       | Proficient with Java and has been following, and active in, the community to understand the rationale behind design decisions and where the project is headed. |

**Issue status**

To communicate the status with other collaborators, you can apply status labels
to issues. These labels prevent duplicating effort.

| **Status**              | **Description**                                                                                                                                                                                                                                                    |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| no status               | Issue has been created, not yet validated by reproducing the claimed bug or not yet accepted due to lack of justification.                                                                                                                                              |
| status/accepted         | Apply to enhancements / feature requests that we think are good to have. Adding this label helps contributors find things to work on. Serves as a backlog.                                                                                                         |
| status/more-info-needed | Apply this to issues that are missing information (eventually we need to update Issue Template that requires Pravega Version, Runtime environment ..), or require feedback from the reporter. If the issue is not updated after a week, it can generally be closed. |
| status/in-progress      | Apply this label if an issue has been picked up by a developer and is in design discussion/development                                                                                                                                                                |
| status/needs-attention  | Apply this label if an issue (or PR) needs more eyes.                                                                                                                                                                                                              |

  
**Issue Estimation**

To plan more accurately and manage expectations of other collaborators, you can
apply estimation labels to issues. These labels will help us estimate and track
overall sprint progress, as well as avoid a developer being overloaded with more than what they could accomplish in a sprint.

 

| **Estimation**             | **Description**                                                                                                                                                      |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| est/1, 3, 5, 8, 13, 21, 40 | Apply these labels to a issue based on how many hours it will take to complete. If an issue takes more than 40 hours, breaking down into manageable pieces is recommended. |

**Issue Workflows**

Although there could be multiple variations in issue transitions, here is a
possible and the most common workflow. 

![issue Workflow](https://github.com/emccode/pravega/blob/master/doc/img/Github%20Issue%20Workflow.png)



### Pull Request Reviews and Labeling

**Status labels**

| **Status**              | **Description**                                                                                                                                                                                                       |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| status/1-design-review  | Maintainers are expected to comment on the design of the pull request. Review of documentation is expected only in the context of design validation, not for stylistic changes.  |
| status/2-code-review    | Maintainers are expected to review the code and ensure that it is of good quality and in accordance with the documentation in the PR.                                             |
| status/3-docs-review    | Maintainers are expected to review the documentation in its bigger context, ensuring consistency, completeness, validity, and breadth of coverage across all existing and new documentation.                          |
| status/4-ready-to-merge | Maintainers are expected to merge this pull request as soon as possible. They can ask for a rebase or carry the pull request themselves. P                                      |
| closed                  | If a pull request is closed it is expected that sufficient justification will be provided.                                                                                                                            |

**PR Workflow**

![PR Workflow](https://github.com/emccode/pravega/blob/master/doc/img/Github%20PR%20Workflow.png)
