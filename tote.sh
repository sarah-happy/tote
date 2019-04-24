#!/bin/sh
export PYTHONPATH="$HOME/tote:$PYTHONPATH"
python3 -m tote "$@"
