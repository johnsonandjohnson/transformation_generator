# How to contribute
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Submitting an issue

Every feature or bug fix should have an associated GitHub issue. This should be created prior to beginning development and, ideally, should be tagged as "accepted" before any actual work is begun.

### Feature Story

A feature story should be written in the behavior-driven development (BDD) process.

> [To learn now about behavior-driven development (BDD)](https://en.wikipedia.org/wiki/Behavior-driven_development)

### Bug Fixes

For bugs, the issue should clearly specify the Steps to reproduce with the Actual behavior. Please be clear about the expected behavior and provide details like device, browser, screenshots or video of the issue.

## Code Unit testing guidelines

UnitTest for Python is used for writing unit test.  Please ensure all code has proper unit test before submission.  

To check run:
```sh
$ python -m unittest
```

## Code Documentation

We use [MKDocs](https://www.mkdocs.org/) for documentation.

When adding a new feature please review the documentation to ensure the new feature is properly documented. 

Bugfix's should review the documentation related to ensure everything is correct and up to date.

Navigate to the `documentation` directory

```sh
$ cd ./documentation
```

While in development run:
```sh
$ mkdocs serve
```

To generate the documentation for a Pull Request (PR):
```sh
$ mkdocs build
```

## Semantic Versioning

We use https://semver.org/ for versioning mettle.

## Commit message format

We use https://www.conventionalcommits.org/en/v1.0.0/ for commit messages.  

## Pull Request (PR)

Once a pull request receives the required approvals, it will be merged to the main branch.

All pull requests must be merged using Squash and merge strategy.

When using Squash and merge strategy, a new squash commit that contains all the changes from the pull request is created by the GitHub. This commit is going to get merged into the main branch.

**IMPORTANT**
The PR title should follow the conventional commit format with the github issue number.  This is important because it will be added to the changelog if it is a `feat:` or `fix:` comment. Below are some examples.

```
fix: Fix text color in the search bar
docs: Improve installation documentation
style: Fix indentation and typos in the files
refactor: Use another logo
test: Add tests for a feature
chore: Add new action Github job
feat: Updated Mettle-tool-tip component with auto placement
```

## Addressing feedback

Please review all feedback on a PR.  This is required for a merge.

## Developer Installation

Now that you have reviewed the contribution guide you are ready to contribute.

- Be sure to fork the repository.
- Clone your forked repository.

Create a virtual environment for the project then activate it.

   ##### Mac / Linux

   ```sh
   $ python -m venv venv

   $ . venv/bin/activate
   ```
   ##### Windows

   ```sh
   $ python -m venv venv

   $ .\venv\Scripts\activate.bat
   ```

Use pip to install all project dependencies into the virtual environment

   ```sh
   $ pip install -r requirements.txt
   ```

Generate antlr files for your environment into the folder `./transform_generator/parser/generated/antlr`

   > NOTE: This step assumes antlr4 is installed on the computer and aliased as "antlr4". See [here](https://github.com/antlr/antlr4/blob/master/doc/getting-started.md) for antlr4 setup instructions.

   ```sh
   $ antlr4 \
      -Dlanguage=Python3 \
      -visitor \
      -o transform_generator/parser/generated \
      antlr/TransformExp.g4
   ```

5. Run the unit tests to verify the install

   ```sh
   $ python -m unittest
   ```

Now that the setup is done
- Create your branch from main.
- Develop!

Happy Contributing!
