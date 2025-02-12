# Data_analytics_platform_on_azure

## `.env` file

Create and `.env` file at your project root, and copy the content of `.env.example` into it.
Just make sure to replace the example values from `.env.example` file with the actual values.

## Setup environment

> Follow these steps in order

### Create virtual environment

Virtual environments aim to isolate project specifics packages from packages installed globally in our computer.

To create a virtual environment, run:
```bash
python -m venv venv
```
To activate the virtual environmen, run:
```bash
venv/Scripts/activate # on windows
source venv/bin/activate # on macOS/Linus
```

### Packages/modules definition with `requirements.txt`

This file list the dependencies of this project.

Run this following command from the root of the project to install dependencies listed in `requirements.txt`
```bash
python install -r requirements.txt
```

## Run app

```bash
python main.py
```
