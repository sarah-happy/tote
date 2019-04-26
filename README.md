# Tote Archive

A duplication eliminating encrypted archive system.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them.

Git

```
sudo apt install git
```

Python 3

```
sudo apt install python3
```

There may be more that I am missing right now.

### Installing

A step by step series of examples that tell you how to get a development env running.

Clone the git repository.

```
git clone https://github.com/sarah-happy/tote.git
```

Adapt tote/tote.sh into a launch script and put it in your path

`$HOME/bin/tote`:

```
#!/bin/sh
export PYTHONPATH="$HOME/tote:$PYTHONPATH"
python3 -m tote "$@"
```

```
chmod +x $HOME/bin/tote
```

End with an example of getting some data out of the system or using it for a little demo

```
tote init

```

## Running the tests

Explain how to run the automated tests for this system.

I haven't written any yet.

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system.

## Versioning

Coming soon.

## Authors

* **Sarah Happy** - *Initial work* - [Sarah_Happy](https://bitbucket.org/sarah_happy/)

## License

I haven't decided on which license to use yet, but it will be something permissive.

## Acknowledgments

* Written after much studying of Borg.

* Written after much studying of Boar.
