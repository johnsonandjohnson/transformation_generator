# Setup and Usage

## Setup
- Clone this repository to the target build machine.

- Generate antlr files for your environment into the folder: `./transform_generator/parser/generated/antlr`
```sh
  $ antlr4 \
    -Dlanguage=Python3 \
    -visitor \
    -o transform_generator/parser/generated \
    antlr/TransformExp.g4
```

- Create a virtual environment for the project then activate it.
```sh
  $ python -m venv venv
  $ . venv/bin/activate
```

- Use pip to install all project dependencies into the virtual environment
```sh
  $ pip install -r requirements.txt
```

- Run the unit tests to verify the install
```sh
  $ python -m unittest
```

## Running the Transformation Generator

The Transformation Generator provides the following execution scripts:

### generate_data_linage.py

```sh
 $ python generate_data_linage.py 
```