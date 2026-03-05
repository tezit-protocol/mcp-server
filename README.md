# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/tezit-protocol/mcp-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                     |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|----------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| src/tez\_server/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |    100% |           |
| src/tez\_server/\_\_main\_\_.py          |        2 |        2 |        0 |        0 |      0% |       1-3 |
| src/tez\_server/server.py                |      153 |        0 |       28 |        0 |    100% |           |
| src/tez\_server/services/\_\_init\_\_.py |        0 |        0 |        0 |        0 |    100% |           |
| src/tez\_server/services/email.py        |       36 |        0 |        6 |        1 |     98% |  20->exit |
| src/tez\_server/services/metadata.py     |       40 |        0 |        8 |        0 |    100% |           |
| src/tez\_server/services/storage.py      |       76 |        1 |       28 |        1 |     98% |        20 |
| src/tez\_server/token\_store.py          |       38 |        1 |        6 |        1 |     95% |        81 |
| **TOTAL**                                |  **345** |    **4** |   **76** |    **3** | **98%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/tezit-protocol/mcp-server/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/tezit-protocol/mcp-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/tezit-protocol/mcp-server/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/tezit-protocol/mcp-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Ftezit-protocol%2Fmcp-server%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/tezit-protocol/mcp-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.